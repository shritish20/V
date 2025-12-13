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
    HARDENED: Includes 'WebSocket Death Spiral' Prevention.
    """
    def __init__(self, rt_quotes: Dict[str, float], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model
        self.token = settings.UPSTOX_ACCESS_TOKEN
        
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
        self._reconnect_attempts = 0
        self._max_backoff = 300
        
        # HARDENING: Circuit Breaker State
        self._consecutive_errors = 0
        self._max_consecutive_errors = 5
        self._circuit_breaker_active = False
        self._circuit_breaker_until = 0

    def subscribe_instrument(self, key: str):
        if not key: return
        if key in self.sub_list: return
        self.sub_list.add(key)
        if self.is_connected and self.streamer:
            try:
                self.streamer.subscribe([key], "ltpc")
                logger.info(f"📡 Subscribed new instrument: {key}")
            except Exception as e:
                logger.error(f"Subscription failed for {key}: {e}")

    def update_token(self, new_token: str):
        if new_token == self.token: return
        logger.info("🔄 Rotating Access Token for WebSocket Feed...")
        self.token = new_token
        self.disconnect()

    def _on_open(self, *args):
        logger.info("🔌 WebSocket Open — subscribing instruments...")
        try:
            self.streamer.subscribe(list(self.sub_list), "ltpc")
            logger.info(f"📡 Subscribed to {len(self.sub_list)} instruments")
            self._consecutive_errors = 0 
            self._circuit_breaker_active = False
        except Exception as e:
            logger.error(f"Subscribe error on open: {e}")
            self._on_error(e)

    def _on_message(self, message, *args):
        self.is_connected = True
        self.last_tick_time = time.time()
        self._reconnect_attempts = 0
        self._consecutive_errors = 0
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
        self._consecutive_errors += 1
        if self._consecutive_errors <= 3:
            logger.debug(f"WS Error: {error}")
        else:
            logger.warning(f"WS Error (attempt {self._consecutive_errors}): {error}")
        
        self.is_connected = False
        
        # CIRCUIT BREAKER LOGIC
        if self._consecutive_errors >= self._max_consecutive_errors:
            logger.critical(
                f"❌ WebSocket failed {self._consecutive_errors} times consecutive. "
                "Pausing for 5 minutes (CIRCUIT BREAKER ACTIVATED)."
            )
            self._circuit_breaker_active = True
            self._circuit_breaker_until = time.time() + 300
            self.disconnect()

    def _on_close(self, code, reason, *args):
        logger.warning(f"WS Closed → code={code}, reason={reason}")
        self.is_connected = False
        self._consecutive_errors += 1

    def _run_feed_process(self):
        try:
            config = upstox_client.Configuration()
            config.access_token = self.token
            config.verify_ssl = False
            config.ssl_ca_cert = None
            
            api_client = upstox_client.ApiClient(config)
            if hasattr(api_client, 'rest_client'):
                api_client.rest_client.pool_manager.connection_pool_kw['timeout'] = 60
                
            self.streamer = MarketDataStreamerV3(api_client)
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            
            logger.info("🔌 Connecting to Upstox V3 WebSocket...")
            self.streamer.connect()
        except Exception as e:
            logger.error(f"Feed crashed: {e}")
            self._on_error(e)
        finally:
            self.is_connected = False
            self._thread_starting = False

    async def _ensure_thread_running(self):
        # 1. CHECK CIRCUIT BREAKER
        if self._circuit_breaker_active:
            remaining = self._circuit_breaker_until - time.time()
            if remaining > 0:
                # Still cooling down
                return
            else:
                logger.info("✅ Circuit Breaker Reset. Attempting reconnect.")
                self._circuit_breaker_active = False
                self._consecutive_errors = 0

        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        async with self._restart_lock:
            if self.feed_thread and self.feed_thread.is_alive(): return
            if self._thread_starting: return

            # Backoff for normal (non-critical) reconnects
            if self._reconnect_attempts > 0:
                backoff = min(2 ** self._reconnect_attempts, self._max_backoff)
                logger.info(f"⏳ Backoff {backoff}s (attempt {self._reconnect_attempts})")
                await asyncio.sleep(backoff)

            self._thread_starting = True
            self.disconnect()
            
            logger.info("🚀 Launching WebSocket thread...")
            self.feed_thread = Thread(target=self._run_feed_process, daemon=True, name="UpstoxV3FeedThread")
            self.feed_thread.start()
            
            # Wait for connection
            for _ in range(15):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    logger.info("✅ WebSocket Connected")
                    self._reconnect_attempts = 0
                    break
            
            if not self.is_connected:
                logger.warning("❌ Connection attempt timed out")
                self._reconnect_attempts += 1
                self._thread_starting = False

    async def start(self):
        logger.info("🚀 Live Data Feed Supervisor Started")
        self.stop_event.clear()
        self.last_tick_time = time.time()
        
        while not self.stop_event.is_set():
            try:
                now = datetime.now(settings.IST).time()
                market_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME
                relaxed_hours = (now.hour >= 7 and now.hour <= 23)
                should_connect = market_open or (settings.SAFETY_MODE != "live" and relaxed_hours)
                
                if should_connect:
                    await self._ensure_thread_running()
                    if time.time() - self.last_tick_time > 90 and self.is_connected:
                        logger.warning("⚠️ Feed stalled for 90s — restarting WebSocket...")
                        self.disconnect()
                        self._reconnect_attempts += 1
                else:
                    if self.is_connected:
                        logger.info("🌙 Night Mode (Market Closed). Disconnecting Feed.")
                        self.disconnect()
                    await asyncio.sleep(60)
                    continue
                    
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Supervisor error: {e}")
                self._reconnect_attempts += 1
                await asyncio.sleep(5)

    async def stop(self):
        logger.info("🛑 Stopping WebSocket Feed...")
        self.stop_event.set()
        self.disconnect()
        await asyncio.sleep(1)

    def disconnect(self):
        try:
            if self.streamer:
                self.streamer.disconnect()
        except Exception:
            pass
        self.streamer = None
        self.is_connected = False
