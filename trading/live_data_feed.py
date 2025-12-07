import asyncio
import time
import logging
from threading import Thread, Event
from typing import Dict, Optional, Set
from upstox_client.feeder.market_data_feed import MarketDataFeed
from upstox_client.feeder.portfolio_data_feed import PortfolioDataFeed

from core.config import settings

logger = logging.getLogger("LiveFeed")

class LiveDataFeed:
    """
    Robust, thread-safe implementation of Upstox Live Feed.
    Features:
    - Auto-reconnection on stale data.
    - Dynamic token updates (Zero Downtime).
    - Thread-safe dictionary updates.
    """
    def __init__(self, rt_quotes: Dict[str, float], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model
        
        # Internal State
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.sub_list: Set[str] = {settings.MARKET_KEY_INDEX, settings.MARKET_KEY_VIX}
        self.feed: Optional[MarketDataFeed] = None
        self.last_tick_time = time.time()
        
        # Concurrency Controls
        self.stop_event = Event()
        self.feed_thread: Optional[Thread] = None
        self.is_connected = False

    def subscribe_instrument(self, key: str):
        """Dynamic subscription safe for runtime calls"""
        if not key or key in self.sub_list:
            return

        self.sub_list.add(key)
        logger.info(f"➕ Subscribing to: {key}")

        # If connected, try to subscribe immediately
        if self.is_connected and self.feed:
            try:
                # The SDK allows dynamic subscription
                self.feed.subscribe([key])
            except Exception as e:
                logger.warning(f"Dynamic subscribe failed (will retry on reconnect): {e}")

    def update_token(self, new_token: str):
        """
        CRITICAL: Allows rotating the access token without killing the bot.
        Triggers a graceful reconnect with the new token.
        """
        if new_token == self.token:
            return

        logger.info("🔄 Rotating Access Token for Live Feed...")
        self.token = new_token
        
        # Force a reconnect cycle
        self.disconnect()
        # The main loop in start() will pick this up and restart with new token

    def on_market_data(self, message):
        """Callback from Upstox SDK (Runs in SDK Thread)"""
        self.last_tick_time = time.time()
        self.is_connected = True

        try:
            if "feeds" in message:
                for key, feed in message["feeds"].items():
                    # Update LTP
                    if "ltpc" in feed:
                        ltp = feed["ltpc"].get("ltp")
                        if ltp:
                            self.rt_quotes[key] = float(ltp)
                            
                    # Note: You could calculate Greeks here, but better to do it 
                    # in the analysis loop to keep this thread lightweight.
        except Exception as e:
            logger.error(f"Feed Parse Error: {e}")

    def on_error(self, error):
        logger.error(f"Upstox Feed Error: {error}")
        self.is_connected = False

    def on_close(self):
        logger.warning("Upstox Feed Connection Closed")
        self.is_connected = False

    def _run_feed_process(self):
        """Blocking process to run in separate thread"""
        try:
            # Re-initialize feed with current token
            self.feed = MarketDataFeed(
                settings.API_BASE_V3,
                self.token,
                instrument_keys=list(self.sub_list)
            )
            
            # Attach callbacks
            self.feed.on_market_data = self.on_market_data
            self.feed.on_error = self.on_error
            self.feed.on_close = self.on_close
            
            logger.info(f"🔌 Connecting Feed with {len(self.sub_list)} instruments...")
            self.feed.connect() # Blocking call
            
        except Exception as e:
            logger.error(f"Feed Thread Crash: {e}")
            self.is_connected = False
        finally:
            logger.info("Feed thread exiting.")

    def disconnect(self):
        """Gracefully disconnect the current feed"""
        if self.feed:
            try:
                self.feed.disconnect()
            except Exception:
                pass
        self.feed = None
        self.is_connected = False

    async def start(self):
        """
        Async Supervisor Loop.
        Monitors the background thread and restarts it if it dies or stalls.
        """
        self.stop_event.clear()
        self.last_tick_time = time.time()

        logger.info("🚀 Live Data Feed Supervisor Started")

        while not self.stop_event.is_set():
            
            # 1. Start Thread if missing
            if self.feed_thread is None or not self.feed_thread.is_alive():
                logger.info("Starting new feed thread...")
                self.feed_thread = Thread(target=self._run_feed_process, daemon=True)
                self.feed_thread.start()
                # Give it a moment to connect
                await asyncio.sleep(2)

            # 2. Watchdog Check
            silence_duration = time.time() - self.last_tick_time
            if silence_duration > 60: # 60 seconds silence
                logger.warning(f"⚠️ Feed Stalled ({silence_duration:.0f}s silence). Restarting...")
                self.disconnect()
                # The loop will restart the thread in next iteration
                self.last_tick_time = time.time() # Reset timer to prevent rapid-fire restarts

            # 3. Sleep check
            await asyncio.sleep(5)

    async def stop(self):
        """Stop the supervisor and feed"""
        logger.info("🛑 Stopping Live Data Feed...")
        self.stop_event.set()
        self.disconnect()
        if self.feed_thread:
            self.feed_thread.join(timeout=2)

