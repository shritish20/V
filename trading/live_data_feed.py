import asyncio
import time
import logging
from threading import Thread, Event
from typing import Dict, Set
import upstox_client
from upstox_client import MarketDataStreamerV3
from core.config import settings

logger = logging.getLogger("LiveFeed")

class LiveDataFeed:
    """
    VolGuard Feed V3.1 (Verified V3 SDK)
    - Uses 'MarketDataStreamerV3'
    - Auto-reconnect enabled via SDK.
    - Timestamps data for Engine safety.
    """
    def __init__(self, rt_quotes: Dict[str, Dict], greeks_cache: Dict, sabr_model):
        self.rt_quotes = rt_quotes
        self.greeks_cache = greeks_cache
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.sub_list: Set[str] = {settings.MARKET_KEY_INDEX, settings.MARKET_KEY_VIX}
        self.streamer = None
        self.stop_event = Event()
        self.is_connected = False

    def subscribe_instrument(self, key: str):
        if not key or key in self.sub_list: return
        self.sub_list.add(key)
        if self.is_connected and self.streamer:
            try:
                self.streamer.subscribe([key], "ltpc")
                logger.info(f"📡 Subscribed: {key}")
            except Exception: pass

    def update_token(self, new_token: str):
        self.token = new_token
        self.disconnect() # Trigger restart

    def _on_open(self, *args):
        logger.info("🔌 WebSocket Connected (V3).")
        self.is_connected = True
        try:
            if self.sub_list:
                self.streamer.subscribe(list(self.sub_list), "ltpc")
        except Exception: pass

    def _on_message(self, message, *args):
        try:
            if "feeds" not in message: return
            now = time.time()
            for key, feed in message["feeds"].items():
                if "ltpc" in feed:
                    ltp = feed["ltpc"].get("ltp")
                    if ltp:
                        # CRITICAL: TIMESTAMP FOR ENGINE
                        # Stores both price and timestamp for stale checks
                        self.rt_quotes[key] = {
                            "ltp": float(ltp),
                            "last_updated": now
                        }
        except Exception: pass

    def _on_error(self, error, *args):
        logger.warning(f"WS Error: {error}")
        self.is_connected = False

    def _on_close(self, *args):
        logger.warning("WS Closed")
        self.is_connected = False

    def _run_streamer(self):
        try:
            config = upstox_client.Configuration()
            config.access_token = self.token
            client = upstox_client.ApiClient(config)
            
            self.streamer = MarketDataStreamerV3(client)
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            
            # Native Auto-Reconnect (Verified)
            self.streamer.auto_reconnect(True, 3, 100)
            self.streamer.connect()
        except Exception as e:
            logger.error(f"Streamer crash: {e}")
            self.is_connected = False

    async def start(self):
        logger.info("🚀 Feed Supervisor Started")
        while not self.stop_event.is_set():
            if not self.is_connected:
                # Launch thread if dead
                t = Thread(target=self._run_streamer, daemon=True, name="UpstoxWS")
                t.start()
                # Wait before checking again
                await asyncio.sleep(5) 
            await asyncio.sleep(1)

    async def stop(self):
        self.stop_event.set()
        self.disconnect()

    def disconnect(self):
        if self.streamer:
            try: self.streamer.disconnect()
            except: pass
        self.is_connected = False
