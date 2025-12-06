import asyncio
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
        self.sub_list.add(key)

    def on_market_data(self, message):
        loop = asyncio.get_event_loop()
        self.last_tick_time = loop.time()
        try:
            if "feeds" in message:
                for key, feed in message["feeds"].items():
                    if "ltpc" in feed:
                        self.rt_quotes[key] = feed["ltpc"]["ltp"]
        except Exception as e:
            logger.error(f"Feed Parse Error: {e}")

    def _start_feed_process(self):
        self.feed = MarketDataFeed(
            self.token, settings.API_BASE_V3, instrument_keys=list(self.sub_list),
        )
        self.feed.on_market_data = self.on_market_data
        self.feed.connect()

    async def start(self):
        self.running = True
        self.last_tick_time = asyncio.get_event_loop().time()
        while self.running:
            logger.info("🔌 Starting Feed Thread...")
            thread = Thread(target=self._start_feed_process, daemon=True)
            thread.start()
            while thread.is_alive() and self.running:
                await asyncio.sleep(1)
            
            if not self.running: break
            logger.warning("⚠️ Feed Thread Died. Restarting...")
            await asyncio.sleep(2)
