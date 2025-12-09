import asyncio
import time
import logging
import traceback
from threading import Thread, Event
from typing import Dict, Optional, Set
from datetime import datetime

import upstox_client
from upstox_client import MarketDataStreamerV3

from core.config import settings

logger = logging.getLogger("LiveFeed")


class LiveDataFeed:
    """
    Upstox V3 MarketDataStreamer Feed (LTPC Mode)
    Robust "Sleep-Aware" version prevents crashes during off-hours.
    """

    def __init__(self, rt_quotes: Dict[str, float], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model

        self.token = settings.UPSTOX_ACCESS_TOKEN

        # Default subscriptions (Index + VIX)
        # Ensure your config.py has MARKET_KEY_VIX = "NSE_INDEX|India VIX" (or similar)
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
        except Exception as e:
            logger.error(f"Subscribe error on open: {e}")

    def _on_message(self, message, *args):
        """Called when a message is received."""
        self.is_connected = True
        self.last_tick_time = time.time()
        self._reconnect_attempts = 0

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
        """Called on error. Accepts extra args to prevent TypeError."""
        logger.debug(f"WS Error: {error}") # Debug level to reduce noise
        self.is_connected = False

    def _on_close(self, code, reason, *args):
        """Called on close. Accepts extra args to prevent TypeError."""
        logger.warning(f"WS Closed → code={code}, reason={reason}")
        self.is_connected = False

    # ---------------------------------------------------------------------
    # THREAD — CONNECT THE WEBSOCKET
    # ---------------------------------------------------------------------
    def _run_feed_process(self):
        try:
            config = upstox_client.Configuration()
            config.access_token = self.token

            self.streamer = MarketDataStreamerV3(
                upstox_client.ApiClient(config)
            )

            # Updated callbacks with resilient signatures
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)

            logger.info("🔌 Connecting to Upstox V3 WebSocket (LTPC Mode)…")
            self.streamer.connect()

        except Exception as e:
            logger.error(f"Feed crashed: {e}")
            logger.debug(traceback.format_exc())
        finally:
            self.is_connected = False
            self._thread_starting = False

    # ---------------------------------------------------------------------
    # THREAD SUPERVISOR + BACKOFF
    # ---------------------------------------------------------------------
    async def _ensure_thread_running(self):
        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        async with self._restart_lock:
            if self.feed_thread and self.feed_thread.is_alive(): return

            if self._thread_starting:
                for _ in range(20): await asyncio.sleep(0.5)
                return

            # Backoff
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
                    break
            
            if not self.is_connected:
                 logger.warning("❌ Connection attempt timed out")
                 self._reconnect_attempts += 1

            self._thread_starting = False

    # ---------------------------------------------------------------------
    # PUBLIC API (Sleep-Aware)
    # ---------------------------------------------------------------------
    async def start(self):
        logger.info("🚀 Live Data Feed Supervisor Started")
        self.stop_event.clear()
        self.last_tick_time = time.time()

        while not self.stop_event.is_set():
            try:
                # 1. CHECK TIME: Only run if Market is Open OR Paper Mode active
                now = datetime.now(settings.IST).time()
                
                # Check strict market hours (9:15 to 3:30)
                market_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME
                
                # Allow relaxed hours for Paper Mode testing (8 AM to 11 PM)
                relaxed_hours = (now.hour >= 8 and now.hour <= 23)
                
                should_connect = market_open or (settings.SAFETY_MODE != "live" and relaxed_hours)

                if should_connect:
                    await self._ensure_thread_running()

                    # Watchdog
                    if time.time() - self.last_tick_time > 60 and self.is_connected:
                        logger.warning("⚠️ Feed stalled — restarting WebSocket…")
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
        try:
            if self.streamer:
                self.streamer.disconnect()
        except Exception:
            pass
        self.streamer = None
        self.is_connected = False
