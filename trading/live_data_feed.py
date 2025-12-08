import asyncio
import time
import logging
import traceback
from threading import Thread, Event
from typing import Dict, Optional, Set
from datetime import datetime
from upstox_client.feeder.market_data_feed import MarketDataFeed

from core.config import settings

logger = logging.getLogger("LiveFeed")

class LiveDataFeed:
    """
    PRODUCTION FIXED:
    - Double-check locking for thread safety
    - Exponential backoff on reconnection failures
    - Prevents API hammering when broker is down
    """
    def __init__(self, rt_quotes: Dict[str, float], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model
        
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.sub_list: Set[str] = {settings.MARKET_KEY_INDEX, settings.MARKET_KEY_VIX}
        self.feed: Optional[MarketDataFeed] = None
        self.last_tick_time = time.time()
        
        self.stop_event = Event()
        self.feed_thread: Optional[Thread] = None
        self.is_connected = False
        
        # Thread safety
        self._restart_lock = asyncio.Lock()
        self._thread_starting = False
        
        # PRODUCTION FIX: Exponential backoff state
        self._reconnect_attempts = 0
        self._max_backoff = 300  # 5 minutes max

    def subscribe_instrument(self, key: str):
        if not key or key in self.sub_list: 
            return
        self.sub_list.add(key)
        if self.is_connected and self.feed:
            try:
                self.feed.subscribe([key])
            except Exception as e:
                logger.debug(f"Subscribe failed: {e}")

    def update_token(self, new_token: str):
        if new_token == self.token: 
            return
        logger.info("🔄 Rotating Access Token for Live Feed...")
        self.token = new_token
        self.disconnect()

    def on_market_data(self, message):
        self.last_tick_time = time.time()
        self.is_connected = True
        
        # PRODUCTION FIX: Reset reconnect counter on successful data
        self._reconnect_attempts = 0
        
        try:
            if "feeds" not in message: 
                return
            for key, feed in message["feeds"].items():
                if "ltpc" in feed:
                    ltp = feed["ltpc"].get("ltp")
                    if ltp:
                        self.rt_quotes[key] = float(ltp)
        except Exception as e:
            logger.debug(f"Data parse error: {e}")

    def on_error(self, error):
        logger.error(f"Upstox Feed Error: {error}")
        self.is_connected = False

    def on_close(self):
        logger.warning("WebSocket closed by broker")
        self.is_connected = False

    def _run_feed_process(self):
        """Blocking method to run in separate thread"""
        try:
            self.feed = MarketDataFeed(
                settings.API_BASE_V3,
                self.token,
                instrument_keys=list(self.sub_list)
            )
            self.feed.on_market_data = self.on_market_data
            self.feed.on_error = self.on_error
            self.feed.on_close = self.on_close
            
            logger.info("🔌 Connecting to Upstox WebSocket...")
            self.feed.connect()  # Blocks until disconnected
            
        except Exception as e:
            logger.error(f"Feed process crashed: {e}")
            logger.debug(traceback.format_exc())
        finally:
            self.is_connected = False
            self._thread_starting = False

    def disconnect(self):
        """Force disconnect and cleanup"""
        if self.feed:
            try:
                self.feed.disconnect()
            except Exception as e:
                logger.debug(f"Disconnect error: {e}")
        
        self.feed = None
        self.is_connected = False

    async def _ensure_thread_running(self):
        """
        PRODUCTION FIX: Double-check locking with exponential backoff.
        """
        # Fast path
        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        # Slow path with lock
        async with self._restart_lock:
            # Double-check inside lock
            if self.feed_thread and self.feed_thread.is_alive():
                return
            
            if self._thread_starting:
                # Wait for other coroutine to finish starting
                for _ in range(20):
                    await asyncio.sleep(0.5)
                    if not self._thread_starting:
                        break
                return

            # PRODUCTION FIX: Apply exponential backoff
            if self._reconnect_attempts > 0:
                backoff = min(2 ** self._reconnect_attempts, self._max_backoff)
                logger.info(
                    f"🔄 Backoff: Waiting {backoff}s before reconnect "
                    f"(attempt {self._reconnect_attempts})"
                )
                await asyncio.sleep(backoff)

            self._thread_starting = True
            
            # Kill zombie threads
            if self.feed_thread is not None:
                self.disconnect()
                if self.feed_thread.is_alive():
                    logger.warning("⚠️ Waiting for zombie thread...")
                    self.feed_thread.join(timeout=5)
                    if self.feed_thread.is_alive():
                        logger.error("❌ Zombie thread won't die.")

            # Spawn new thread
            logger.info("🚀 Starting Feed Thread...")
            self.feed_thread = Thread(
                target=self._run_feed_process, 
                daemon=True,
                name="UpstoxFeedThread"
            )
            self.feed_thread.start()
            
            # Wait for connection or failure
            for _ in range(20):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    logger.info("✅ Feed Connected")
                    self._reconnect_attempts = 0  # Reset on success
                    break
                if not self.feed_thread.is_alive():
                    logger.error("❌ Feed thread died immediately")
                    self._reconnect_attempts += 1
                    self._thread_starting = False
                    break
            else:
                # Timeout
                if not self.is_connected:
                    logger.warning("⚠️ Connection timeout (10s)")
                    self._reconnect_attempts += 1
            
            self._thread_starting = False

    async def start(self):
        """Main supervisor loop with backoff logic"""
        self.stop_event.clear()
        self.last_tick_time = time.time()
        logger.info("🚀 Live Data Feed Supervisor Started")

        while not self.stop_event.is_set():
            try:
                # Market hours check
                now = datetime.now(settings.IST).time()
                is_market_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME
                
                should_run = is_market_open or settings.SAFETY_MODE != "live"

                if should_run:
                    await self._ensure_thread_running()

                    # Watchdog: Restart if stalled
                    tick_age = time.time() - self.last_tick_time
                    if tick_age > 60 and self.is_connected:
                        logger.warning(f"⚠️ Feed Stalled ({tick_age:.0f}s). Restarting...")
                        self.disconnect()
                        async with self._restart_lock:
                            self.feed_thread = None
                        # Increment reconnect counter
                        self._reconnect_attempts += 1
                        self.last_tick_time = time.time()
                else:
                    # Market closed
                    if self.is_connected:
                        logger.info("🌙 Market Closed. Pausing Feed.")
                        self.disconnect()
                        # Reset backoff during market closure
                        self._reconnect_attempts = 0

            except Exception as e:
                logger.error(f"Supervisor Loop Error: {e}")
                logger.debug(traceback.format_exc())
                self._reconnect_attempts += 1
            
            await asyncio.sleep(5)

    async def stop(self):
        """Graceful shutdown"""
        logger.info("🛑 Stopping Live Data Feed...")
        self.stop_event.set()
        self.disconnect()
        
        if self.feed_thread and self.feed_thread.is_alive():
            logger.info("Waiting for feed thread to exit...")
            await asyncio.sleep(2)
