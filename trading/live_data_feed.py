import asyncio
import time
import logging
from threading import Thread, Event
from typing import Dict, Optional, Set
from datetime import datetime
from upstox_client.feeder.market_data_feed import MarketDataFeed

from core.config import settings

logger = logging.getLogger("LiveFeed")

class LiveDataFeed:
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

    def subscribe_instrument(self, key: str):
        if not key or key in self.sub_list: return
        self.sub_list.add(key)
        if self.is_connected and self.feed:
            try:
                self.feed.subscribe([key])
            except Exception:
                pass

    def update_token(self, new_token: str):
        if new_token == self.token: return
        logger.info("🔄 Rotating Access Token for Live Feed...")
        self.token = new_token
        self.disconnect()

    def on_market_data(self, message):
        self.last_tick_time = time.time()
        self.is_connected = True
        try:
            if "feeds" not in message: return
            for key, feed in message["feeds"].items():
                if "ltpc" in feed:
                    ltp = feed["ltpc"].get("ltp")
                    if ltp:
                        self.rt_quotes[key] = float(ltp)
        except Exception:
            pass

    def on_error(self, error):
        logger.error(f"Upstox Feed Error: {error}")
        self.is_connected = False

    def on_close(self):
        self.is_connected = False

    def _run_feed_process(self):
        try:
            self.feed = MarketDataFeed(
                settings.API_BASE_V3,
                self.token,
                instrument_keys=list(self.sub_list)
            )
            self.feed.on_market_data = self.on_market_data
            self.feed.on_error = self.on_error
            self.feed.on_close = self.on_close
            self.feed.connect()
        except Exception:
            self.is_connected = False

    def disconnect(self):
        if self.feed:
            try: self.feed.disconnect()
            except Exception: pass
        self.feed = None
        self.is_connected = False

    async def start(self):
        self.stop_event.clear()
        self.last_tick_time = time.time()
        logger.info("🚀 Live Data Feed Supervisor Started")

        while not self.stop_event.is_set():
            # FIX: Market Hours Check
            now = datetime.now(settings.IST).time()
            is_market_open = settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME
            
            # Only force restart if market is open or we are in testing/shadow mode
            should_run = is_market_open or settings.SAFETY_MODE != "live"

            if should_run:
                if self.feed_thread is None or not self.feed_thread.is_alive():
                    self.feed_thread = Thread(target=self._run_feed_process, daemon=True)
                    self.feed_thread.start()
                    await asyncio.sleep(2)

                # Watchdog
                if time.time() - self.last_tick_time > 60:
                    logger.warning("⚠️ Feed Stalled. Restarting...")
                    self.disconnect()
                    self.last_tick_time = time.time()
            else:
                # Market Closed
                if self.is_connected:
                    logger.info("🌙 Market Closed. Pausing Feed.")
                    self.disconnect()

            await asyncio.sleep(5)

    async def stop(self):
        self.stop_event.set()
        self.disconnect()
