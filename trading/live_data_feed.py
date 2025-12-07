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
    PRODUCTION-READY: Fixed WebSocket Race Condition with Double-Check Locking
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
        
        # CRITICAL FIX: Async lock with atomic state tracking
        self._restart_lock = asyncio.Lock()
        self._thread_starting = False  # Prevents duplicate starts

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
        """
        Blocking method to run in a separate thread.
        """
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
        PRODUCTION FIX: Double-Check Locking Pattern
        Prevents race conditions during thread spawning.
        """
        # Fast path: Check without lock
        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return  # Already running

        # Slow path: Acquire lock and re-check
        async with self._restart_lock:
            # Double-check inside lock
            if self.feed_thread and self.feed_thread.is_alive():
                return  # Another coroutine already started it
            
            if self._thread_starting:
                # Another coroutine is currently starting the thread
                # Wait for it to complete
                for _ in range(20):  # Max 10 seconds
                    await asyncio.sleep(0.5)
                    if not self._thread_starting:
                        break
                return

            # Mark as starting (prevents duplicate attempts)
            self._thread_starting = True
            
            # Kill zombie threads
            if self.feed_thread is not None:
                self.disconnect()
                if self.feed_thread.is_alive():
                    logger.warning("⚠️ Waiting for zombie thread to die...")
                    self.feed_thread.join(timeout=5)
                    if self.feed_thread.is_alive():
                        logger.error("❌ Zombie thread won't die. Proceeding anyway.")

            # Spawn new thread
            logger.info("🚀 Starting Feed Thread...")
            self.feed_thread = Thread(
                target=self._run_feed_process, 
                daemon=True,
                name="UpstoxFeedThread"
            )
            self.feed_thread.start()
            
            # Wait for connection OR failure (max 10 seconds)
            for _ in range(20):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    logger.info("✅ Feed Connected")
                    break
                if not self.feed_thread.is_alive():
                    logger.error("❌ Feed thread died immediately")
                    self._thread_starting = False
                    break
            else:
                # Timeout - connection didn't establish in 10s
                if not self.is_connected:
                    logger.warning("⚠️ Connection timeout (10s). Will retry.")
            
            self._thread_starting = False

    async def start(self):
        """
        Main supervisor loop. Manages the background thread safely.
        """
        self.stop_event.clear()
        self.last_tick_time = time.time()
        logger.info("🚀 Live Data Feed Supervisor Started")

        while not self.stop_event.is_set():
            try:
                # Market Hours Check
                now = datetime.now(settings.IST).time()
                is_market_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME
                
                # Run feed during market hours or in testing mode
                should_run = is_market_open or settings.SAFETY_MODE != "live"

                if should_run:
                    # PRODUCTION FIX: Use atomic thread manager
                    await self._ensure_thread_running()

                    # Watchdog: Restart if no ticks for 60 seconds
                    tick_age = time.time() - self.last_tick_time
                    if tick_age > 60 and self.is_connected:
                        logger.warning(f"⚠️ Feed Stalled ({tick_age:.0f}s). Triggering Restart...")
                        self.disconnect()
                        # Force thread cleanup
                        async with self._restart_lock:
                            self.feed_thread = None
                        # Reset timer to avoid rapid restart loops
                        self.last_tick_time = time.time()
                else:
                    # Market Closed
                    if self.is_connected:
                        logger.info("🌙 Market Closed. Pausing Feed.")
                        self.disconnect()

            except Exception as e:
                logger.error(f"Supervisor Loop Error: {e}")
                logger.debug(traceback.format_exc())
            
            await asyncio.sleep(5)

    async def stop(self):
        """Graceful shutdown"""
        logger.info("🛑 Stopping Live Data Feed...")
        self.stop_event.set()
        self.disconnect()
        
        # Wait for thread to finish
        if self.feed_thread and self.feed_thread.is_alive():
            logger.info("Waiting for feed thread to exit...")
            await asyncio.sleep(2)
