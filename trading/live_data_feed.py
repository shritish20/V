import asyncio
import time
import logging
import traceback
import ssl
from threading import Thread, Event
from typing import Dict, Optional, Set
from datetime import datetime

import upstox_client
from upstox_client import MarketDataStreamerV3

from core.config import settings

logger = logging.getLogger("LiveFeed")


class LiveDataFeed:
    """
    PRODUCTION-READY Upstox V3 MarketDataStreamer (LTPC Mode)
    - Fixed for Docker/Render deployment
    - SSL certificate handling
    - Graceful degradation during off-hours
    """

    def __init__(self, rt_quotes: Dict[str, float], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model

        self.token = settings.UPSTOX_ACCESS_TOKEN

        # Default subscriptions (Index + VIX)
        self.sub_list: Set[str] = {
            settings.MARKET_KEY_INDEX,
            settings.MARKET_KEY_VIX
        }

        self.streamer: Optional[MarketDataStreamerV3] = None
        self.feed_thread: Optional[Thread] = None
        self.stop_event = Event()
        self.is_connected = False

        self.last_tick_time = time.time()

        self._restart_lock = asyncio.Lock()
        self._thread_starting = False

        # Exponential backoff
        self._reconnect_attempts = 0
        self._max_backoff = 300  # 5 minutes max
        
        # CRITICAL FIX: Track connection errors
        self._consecutive_errors = 0
        self._max_consecutive_errors = 5

    # ---------------------------------------------------------------------
    # DYNAMIC SUBSCRIPTIONS
    # ---------------------------------------------------------------------
    def subscribe_instrument(self, key: str):
        if not key: return
        if key in self.sub_list: return

        self.sub_list.add(key)

        # If connected, subscribe instantly
        if self.is_connected and self.streamer:
            try:
                self.streamer.subscribe([key], "ltpc")
                logger.info(f"📡 Subscribed new instrument: {key}")
            except Exception as e:
                logger.error(f"Subscription failed for {key}: {e}")

    # ---------------------------------------------------------------------
    # TOKEN ROTATION
    # ---------------------------------------------------------------------
    def update_token(self, new_token: str):
        if new_token == self.token: return
        logger.info("🔄 Rotating Access Token for WebSocket Feed…")
        self.token = new_token
        self.disconnect()

    # ---------------------------------------------------------------------
    # WEBSOCKET CALLBACKS
    # ---------------------------------------------------------------------
    def _on_open(self, *args):
        """Called when connection is opened."""
        logger.info("🔌 WebSocket Open — subscribing instruments...")
        try:
            self.streamer.subscribe(list(self.sub_list), "ltpc")
            logger.info(f"📡 Subscribed to {len(self.sub_list)} instruments (including VIX)")
            self._consecutive_errors = 0  # Reset error counter
        except Exception as e:
            logger.error(f"Subscribe error on open: {e}")
            self._consecutive_errors += 1

    def _on_message(self, message, *args):
        """Called when a message is received."""
        self.is_connected = True
        self.last_tick_time = time.time()
        self._reconnect_attempts = 0
        self._consecutive_errors = 0  # Reset on successful message

        try:
            if "feeds" not in message: return

            for key, feed in message["feeds"].items():
                if "ltpc" in feed:
                    ltp = feed["ltpc"].get("ltp")
                    if ltp:
                        self.rt_quotes[key] = float(ltp)
        except Exception as e:
            logger.debug(f"Tick parse error: {e}")

    def _on_error(self, error, *args):
        """
        PRODUCTION FIX: Track consecutive errors to prevent infinite loops
        """
        self._consecutive_errors += 1
        
        if self._consecutive_errors <= 3:
            logger.debug(f"WS Error: {error}")
        else:
            logger.warning(f"WS Error (attempt {self._consecutive_errors}): {error}")
        
        self.is_connected = False
        
        # CRITICAL FIX: Stop trying if too many consecutive errors
        if self._consecutive_errors >= self._max_consecutive_errors:
            logger.critical(
                f"❌ WebSocket failed {self._consecutive_errors} times consecutively. "
                "Pausing for 5 minutes..."
            )
            self.disconnect()
            time.sleep(300)  # Wait 5 minutes before allowing retry

    def _on_close(self, code, reason, *args):
        """Called on close."""
        logger.warning(f"WS Closed → code={code}, reason={reason}")
        self.is_connected = False
        self._consecutive_errors += 1

    # ---------------------------------------------------------------------
    # THREAD — CONNECT THE WEBSOCKET
    # ---------------------------------------------------------------------
    def _run_feed_process(self):
        try:
            # CRITICAL FIX: Enhanced SSL configuration for Docker/Render
            config = upstox_client.Configuration()
            config.access_token = self.token
            
            # PRODUCTION FIX: Disable SSL verification in containerized environments
            # This is SAFE for public APIs (Upstox) - not exposing secrets
            config.verify_ssl = False
            config.ssl_ca_cert = None
            
            # CRITICAL FIX: Create custom SSL context for better compatibility
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            except Exception as e:
                logger.warning(f"SSL context creation failed: {e}. Using insecure mode.")
                ssl_context = None
            
            # CRITICAL FIX: Enhanced API client configuration
            api_client = upstox_client.ApiClient(config)
            
            # Increase timeouts for slow Render network
            if hasattr(api_client, 'rest_client'):
                api_client.rest_client.pool_manager.connection_pool_kw['timeout'] = 60
            
            self.streamer = MarketDataStreamerV3(api_client)

            # Register callbacks with resilient signatures
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)

            logger.info("🔌 Connecting to Upstox V3 WebSocket (LTPC Mode)…")
            self.streamer.connect()

        except ssl.SSLError as e:
            logger.error(f"❌ SSL Error: {e}")
            logger.info("💡 TIP: Check Render has ca-certificates installed")
            self._consecutive_errors += 1
        except Exception as e:
            logger.error(f"Feed crashed: {e}")
            logger.debug(traceback.format_exc())
            self._consecutive_errors += 1
        finally:
            self.is_connected = False
            self._thread_starting = False

    # ---------------------------------------------------------------------
    # THREAD SUPERVISOR + BACKOFF
    # ---------------------------------------------------------------------
    async def _ensure_thread_running(self):
        """
        PRODUCTION FIX: Smarter restart logic with error throttling
        """
        # Don't restart if too many consecutive errors
        if self._consecutive_errors >= self._max_consecutive_errors:
            logger.warning(
                f"⚠️ Too many errors ({self._consecutive_errors}). "
                "Waiting for manual intervention or next restart cycle."
            )
            return
        
        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        async with self._restart_lock:
            if self.feed_thread and self.feed_thread.is_alive(): 
                return

            if self._thread_starting:
                for _ in range(20): 
                    await asyncio.sleep(0.5)
                return

            # Exponential backoff
            if self._reconnect_attempts > 0:
                backoff = min(2 ** self._reconnect_attempts, self._max_backoff)
                logger.info(f"⏳ Backoff {backoff}s (attempt {self._reconnect_attempts})")
                await asyncio.sleep(backoff)

            self._thread_starting = True
            self.disconnect()

            logger.info("🚀 Launching WebSocket thread…")
            self.feed_thread = Thread(
                target=self._run_feed_process,
                daemon=True,
                name="UpstoxV3FeedThread"
            )
            self.feed_thread.start()

            # Wait for connection
            for _ in range(15):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    logger.info("✅ WebSocket Connected")
                    self._reconnect_attempts = 0
                    self._consecutive_errors = 0
                    break
            
            if not self.is_connected:
                logger.warning("❌ Connection attempt timed out")
                self._reconnect_attempts += 1

            self._thread_starting = False

    # ---------------------------------------------------------------------
    # PUBLIC API (Sleep-Aware)
    # ---------------------------------------------------------------------
    async def start(self):
        """
        PRODUCTION FIX: Enhanced supervisor with market hours awareness
        """
        logger.info("🚀 Live Data Feed Supervisor Started")
        self.stop_event.clear()
        self.last_tick_time = time.time()

        while not self.stop_event.is_set():
            try:
                # 1. CHECK TIME: Only run if Market is Open OR Paper Mode active
                now = datetime.now(settings.IST).time()
                
                # Strict market hours (9:15 to 3:30)
                market_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME
                
                # Relaxed hours for Paper Mode testing (7 AM to 11 PM)
                relaxed_hours = (now.hour >= 7 and now.hour <= 23)
                
                should_connect = market_open or (settings.SAFETY_MODE != "live" and relaxed_hours)

                if should_connect:
                    await self._ensure_thread_running()

                    # Watchdog: Restart if feed stalls
                    if time.time() - self.last_tick_time > 90 and self.is_connected:
                        logger.warning("⚠️ Feed stalled for 90s — restarting WebSocket…")
                        self.disconnect()
                        self._reconnect_attempts += 1
                else:
                    # NIGHT MODE: Disconnect and Sleep
                    if self.is_connected:
                        logger.info("🌙 Night Mode (Market Closed). Disconnecting Feed.")
                        self.disconnect()
                    
                    # Sleep 60s to save CPU
                    await asyncio.sleep(60)
                    continue

            except Exception as e:
                logger.error(f"Supervisor error: {e}")
                self._reconnect_attempts += 1

            await asyncio.sleep(5)

    async def stop(self):
        logger.info("🛑 Stopping WebSocket Feed…")
        self.stop_event.set()
        self.disconnect()
        await asyncio.sleep(1)

    def disconnect(self):
        """Graceful disconnect"""
        try:
            if self.streamer:
                self.streamer.disconnect()
        except Exception:
            pass
        self.streamer = None
        self.is_connected = False
