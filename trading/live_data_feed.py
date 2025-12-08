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
    Upstox V3 MarketDataStreamer Feed.
    Includes 'Night Mode' to prevent crash loops during off-hours.
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

    def subscribe_instrument(self, key: str):
        if not key or key in self.sub_list: return
        self.sub_list.add(key)
        if self.is_connected and self.streamer:
            try:
                self.streamer.subscribe([key], "ltpc")
            except Exception:
                pass

    def update_token(self, new_token: str):
        if new_token == self.token: return
        logger.info("🔄 Rotating Access Token...")
        self.token = new_token
        self.disconnect()

    # --- WEBSOCKET CALLBACKS ---
    def _on_open(self):
        logger.info("🔌 WebSocket Connected")
        self.is_connected = True
        self._reconnect_attempts = 0
        try:
            self.streamer.subscribe(list(self.sub_list), "ltpc")
        except Exception:
            pass

    def _on_message(self, message):
        self.last_tick_time = time.time()
        try:
            if "feeds" not in message: return
            for key, feed in message["feeds"].items():
                if "ltpc" in feed:
                    ltp = feed["ltpc"].get("ltp")
                    if ltp: self.rt_quotes[key] = float(ltp)
        except Exception:
            pass

    def _on_error(self, ws, error):
        logger.debug(f"WS Error: {error}")
        self.is_connected = False

    def _on_close(self, ws, code, reason):
        self.is_connected = False

    # --- THREAD LOGIC ---
    def _run_feed_process(self):
        """Blocking process run in thread."""
        try:
            config = upstox_client.Configuration()
            config.access_token = self.token
            self.streamer = MarketDataStreamerV3(upstox_client.ApiClient(config))
            
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            
            self.streamer.connect() # Blocks here
        except Exception as e:
            logger.error(f"Feed Thread Exception: {e}")
        finally:
            self.is_connected = False
            self._thread_starting = False

    def disconnect(self):
        try:
            if self.streamer:
                self.streamer.disconnect()
        except Exception:
            pass
        self.streamer = None
        self.is_connected = False

    # --- SUPERVISOR ---
    async def _ensure_thread_running(self):
        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        async with self._restart_lock:
            if self.feed_thread and self.feed_thread.is_alive(): return

            # Backoff to prevent rapid restart loops
            if self._reconnect_attempts > 0:
                backoff = min(2 ** self._reconnect_attempts, 60)
                await asyncio.sleep(backoff)

            self._thread_starting = True
            self.disconnect()

            self.feed_thread = Thread(target=self._run_feed_process, daemon=True, name="UpstoxV3Feed")
            self.feed_thread.start()

            # Wait for connection
            for _ in range(15):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    self._reconnect_attempts = 0
                    return

            self._reconnect_attempts += 1
            self._thread_starting = False

    async def start(self):
        logger.info("🚀 Live Data Feed Supervisor Started")
        self.stop_event.clear()
        
        while not self.stop_event.is_set():
            try:
                now = datetime.now(settings.IST).time()
                
                # CRITICAL: Only run if market is open OR we are in paper mode (for testing)
                # But strict check: If it's 4 AM, even Paper mode can't connect to Upstox WS
                # So we check if we are loosely within "Daytime" (e.g., 8 AM to 11:30 PM)
                # Or just check settings.MARKET_OPEN/CLOSE
                
                is_market_hours = (settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME)
                
                # Allow connection if market open OR (Paper Mode AND it is reasonable daytime)
                should_connect = is_market_hours or (
                    settings.SAFETY_MODE != "live" and 
                    (now.hour >= 8 and now.hour <= 23)
                )

                if should_connect:
                    await self._ensure_thread_running()
                    # Watchdog
                    if time.time() - self.last_tick_time > 60 and self.is_connected:
                        logger.warning("⚠️ Feed Stalled. Restarting...")
                        self.disconnect()
                else:
                    # Night Mode: Disconnect and Sleep
                    if self.is_connected:
                        logger.info("🌙 Night Mode. Disconnecting Feed.")
                        self.disconnect()
                    
                    # Sleep longer at night to save resources
                    await asyncio.sleep(60)
                    continue

            except Exception as e:
                logger.error(f"Supervisor Error: {e}")
                await asyncio.sleep(5)
            
            await asyncio.sleep(5)

    async def stop(self):
        self.stop_event.set()
        self.disconnect()
