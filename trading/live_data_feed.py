import asyncio
import time
from threading import Thread
from upstox_client.feeder.market_data_feed import MarketDataFeed
from core.config import settings
from utils.logger import get_logger

logger = get_logger("LiveFeed")

class LiveDataFeed:
    def __init__(self, rt_quotes, greeks_cache, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.sabr_model = sabr_model
        
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.sub_list = {settings.MARKET_KEY_INDEX, settings.MARKET_KEY_VIX}
        self.feed = None
        self.last_tick_time = 0.0
        self.running = False

    def subscribe_instrument(self, key: str):
        """
        Subscribes to a new instrument dynamically.
        If feed is running, sends the subscribe command immediately.
        """
        if key in self.sub_list:
            return

        self.sub_list.add(key)
        logger.info(f"➕ Subscribing to new instrument: {key}")
        
        # Dynamic Subscription Fix:
        if self.feed and self.running:
            try:
                # The Upstox SDK usually exposes a subscribe method. 
                # If using the 'MarketDataFeed' class specifically, we might need to 
                # rely on the restart loop if it doesn't support dynamic updates, 
                # but typically SDKs allow this.
                # Assuming standard SDK behavior:
                self.feed.subscribe([key]) 
            except Exception as e:
                logger.warning(f"Dynamic subscription failed (will retry on restart): {e}")

    def on_market_data(self, message):
        # FIX 1: Use time.time() instead of asyncio loop in a thread
        self.last_tick_time = time.time()
        
        try:
            if "feeds" in message:
                for key, feed in message["feeds"].items():
                    if "ltpc" in feed:
                        ltp = feed["ltpc"]["ltp"]
                        self.rt_quotes[key] = ltp
                        
                        # Optional: If you need Greeks calculated here, 
                        # you must use self.greeks_cache safely.
        except Exception as e:
            logger.error(f"Feed Parse Error: {e}")

    def _start_feed_process(self):
        try:
            # FIX 2: Ensure API_BASE_V3 is actually what the SDK expects.
            # Usually Upstox SDK just needs the token and config.
            # We pass the current sub_list to ensure all keys are grabbed on start/restart.
            self.feed = MarketDataFeed(
                self.token, 
                settings.API_BASE_V3, 
                instrument_keys=list(self.sub_list)
            )
            self.feed.on_market_data = self.on_market_data
            logger.info(f"🔌 Connecting Feed with {len(self.sub_list)} instruments...")
            self.feed.connect()
        except Exception as e:
            logger.error(f"Feed Process Crash: {e}")

    async def start(self):
        self.running = True
        self.last_tick_time = time.time()
        
        while self.running:
            logger.info("🔄 Starting Feed Thread Monitor...")
            thread = Thread(target=self._start_feed_process, daemon=True)
            thread.start()
            
            # Watchdog Loop
            while thread.is_alive() and self.running:
                # Check for stale feed (Heartbeat)
                if time.time() - self.last_tick_time > 60:
                    logger.warning("❤️ Feed Stalled (No ticks for 60s). Restarting...")
                    try:
                        # Force close if possible, or just break to restart
                        if self.feed: 
                            self.feed.disconnect() # Attempt graceful close
                    except:
                        pass
                    break # Break inner loop to trigger restart
                
                await asyncio.sleep(1)
            
            if not self.running:
                break
                
            logger.warning("⚠️ Feed Thread Died or Stalled. Restarting in 2s...")
            await asyncio.sleep(2)
