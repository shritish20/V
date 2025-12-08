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
    PRODUCTION READY — Upgraded to Upstox MarketDataStreamerV3 (LTPC MODE)

    - Thread-safe
    - Exponential backoff on reconnection
    - Watchdog for stalled feed
    - Token rotation support
    - Same interface as earlier
    """

    def __init__(self, rt_quotes: Dict[str, float], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model

        self.token = settings.UPSTOX_ACCESS_TOKEN

        # Default subscriptions — Index + VIX
        self.sub_list: Set[str] = {
            settings.MARKET_KEY_INDEX,
            settings.MARKET_KEY_VIX
        }

        self.feed: Optional[MarketDataStreamerV3] = None
        self.feed_thread: Optional[Thread] = None

        self.stop_event = Event()
        self.is_connected = False
        self.last_tick_time = time.time()

        # Thread-safety locks
        self._restart_lock = asyncio.Lock()
        self._thread_starting = False

        # Reconnect backoff
        self._reconnect_attempts = 0
        self._max_backoff = 300  # 5 mins max

    # ---------------------------------------------------------------------
    # SUBSCRIPTIONS
    # ---------------------------------------------------------------------
    def subscribe_instrument(self, key: str):
        """Add instrument dynamically"""
        if not key or key in self.sub_list:
            return

        self.sub_list.add(key)

        if self.is_connected and self.feed:
            try:
                self.feed.subscribe([key], "ltpc")
            except Exception as e:
                logger.debug(f"Subscribe failed: {e}")

    # ---------------------------------------------------------------------
    # TOKEN ROTATION
    # ---------------------------------------------------------------------
    def update_token(self, new_token: str):
        if new_token == self.token:
            return

        logger.info("🔄 Rotating Access Token for Live Feed...")
        self.token = new_token
        self.disconnect()

    # ---------------------------------------------------------------------
    # EVENT HANDLERS
    # ---------------------------------------------------------------------
    def _on_open(self):
        """Called when WebSocket connects."""
        try:
            logger.info("🌐 WebSocket Open — subscribing instruments")
            self.feed.subscribe(list(self.sub_list), "ltpc")
        except Exception as e:
            logger.error(f"Subscription error on open: {e}")

    def _on_message(self, message):
        """Market tick handler."""
        self.is_connected = True
        self.last_tick_time = time.time()
        self._reconnect_attempts = 0

        try:
            # Example message format from Upstox:
            # { "data": { "ltpc": { "ltp": 12345 }, ... }, "instrument_key": "NSE_INDEX|Nifty 50" }
            if "data" in message and "ltpc" in message["data"]:
                ltp = message["data"]["ltpc"].get("ltp")
                key = message.get("instrument_key")
                if ltp and key:
                    self.rt_quotes[key] = float(ltp)
        except Exception as e:
            logger.debug(f"Parse error: {e}")

    def _on_error(self, message):
        logger.error(f"Feed Error: {message}")
        self.is_connected = False

    def _on_close(self):
        logger.warning("WebSocket closed")
        self.is_connected = False

    # ---------------------------------------------------------------------
    # FEED THREAD
    # ---------------------------------------------------------------------
    def _run_feed_process(self):
        """Blocking process inside a thread."""
        try:
            config = upstox_client.Configuration()
            config.access_token = self.token

            api_client = upstox_client.ApiClient(config)

            self.feed = MarketDataStreamerV3(api_client)

            # Bind callbacks
            self.feed.on("open", self._on_open)
            self.feed.on("message", self._on_message)
            self.feed.on("error", self._on_error)
            self.feed.on("close", self._on_close)

            logger.info("🔌 Connecting to Upstox WebSocket LTPC feed...")

            # Connect (blocks)
            self.feed.connect()

        except Exception as e:
            logger.error(f"Feed crashed: {e}")
            logger.debug(traceback.format_exc())
        finally:
            self.is_connected = False
            self._thread_starting = False

    # ---------------------------------------------------------------------
    def disconnect(self):
        """Terminate WebSocket cleanly."""
        if self.feed:
            try:
                self.feed.disconnect()
            except Exception:
                pass

        self.feed = None
        self.is_connected = False

    # ---------------------------------------------------------------------
    async def _ensure_thread_running(self):
        """Ensures the feed thread is active, with backoff."""
        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        async with self._restart_lock:

            if self.feed_thread and self.feed_thread.is_alive():
                return

            if self._thread_starting:
                for _ in range(20):
                    await asyncio.sleep(0.5)
                    if not self._thread_starting:
                        break
                return

            # Apply exponential backoff
            if self._reconnect_attempts > 0:
                backoff = min(2 ** self._reconnect_attempts, self._max_backoff)
                logger.info(f"⏳ Backoff {backoff}s (attempt {self._reconnect_attempts})")
                await asyncio.sleep(backoff)

            self._thread_starting = True

            # Kill zombie thread
            if self.feed_thread:
                self.disconnect()
                if self.feed_thread.is_alive():
                    self.feed_thread.join(timeout=5)

            # Start new feed thread
            logger.info("🚀 Spawning WebSocket thread...")
            self.feed_thread = Thread(
                target=self._run_feed_process,
                daemon=True,
                name="UpstoxFeedThread"
            )
            self.feed_thread.start()

            # Wait for it to connect
            for _ in range(20):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    logger.info("✅ LTPC Feed Connected")
                    self._reconnect_attempts = 0
                    break

                if not self.feed_thread.is_alive():
                    logger.error("❌ Feed thread died immediately")
                    self._reconnect_attempts += 1
                    break

            self._thread_starting = False

    # ---------------------------------------------------------------------
    async def start(self):
        """Supervisor loop with stall watchdog."""
        self.stop_event.clear()
        self.last_tick_time = time.time()

        logger.info("🚀 Live Data Feed Supervisor Started")

        while not self.stop_event.is_set():
            try:
                now = datetime.now(settings.IST).time()
                is_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME

                should_run = is_open or settings.SAFETY_MODE != "live"

                if should_run:
                    await self._ensure_thread_running()

                    # Stall watchdog
                    tick_age = time.time() - self.last_tick_time
                    if tick_age > 60 and self.is_connected:
                        logger.warning(f"⚠️ Feed Stalled ({tick_age:.0f}s). Restarting...")
                        self.disconnect()
                        async with self._restart_lock:
                            self.feed_thread = None
                        self._reconnect_attempts += 1
                        self.last_tick_time = time.time()

                else:
                    if self.is_connected:
                        logger.info("🌙 Market Closed — disconnecting feed")
                        self.disconnect()
                        self._reconnect_attempts = 0

            except Exception as e:
                logger.error(f"Supervisor error: {e}")
                self._reconnect_attempts += 1

            await asyncio.sleep(5)

    # ---------------------------------------------------------------------
    async def stop(self):
        logger.info("🛑 Stopping Live Feed...")
        self.stop_event.set()
        self.disconnect()

        if self.feed_thread and self.feed_thread.is_alive():
            await asyncio.sleep(2)
