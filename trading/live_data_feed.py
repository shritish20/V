import asyncio
import time
import logging
import traceback
import ssl
from threading import Thread, Event
from typing import Dict, Optional, Set
from datetime import datetime

import upstox_client
from upstox_client.rest import ApiException
from upstox_client import MarketDataStreamerV3
from core.config import settings

logger = logging.getLogger("LiveFeed")

class LiveDataFeed:
    """
    VolGuard 20.0 - Upstox V3 Protobuf Optimized
    - MANDATORY: Decodes V3 binary protobuf ticks.
    - RELAXED: Circuit breaker for 2025 connection stability.
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
        self._max_backoff = 60 
        
        # Hardened Circuit Breaker
        self._consecutive_errors = 0
        self._max_consecutive_errors = 15
        self._circuit_breaker_active = False
        self._circuit_breaker_until = 0

    def subscribe_instrument(self, key: str):
        """Adds instrument to V3 subscription list."""
        if not key or key in self.sub_list: return
        self.sub_list.add(key)
        if self.is_connected and self.streamer:
            try:
                # Mode 'ltpc' is the default for V3 binary stream
                self.streamer.subscribe([key], "ltpc")
                logger.info(f"📡 V3 Subscribed: {key}")
            except Exception as e:
                logger.error(f"V3 Subscription failed for {key}: {e}")

    def update_token(self, new_token: str):
        if new_token == self.token: return
        logger.info("🔄 Rotating Token for V3 Feed...")
        self.token = new_token
        self.disconnect()

    def _on_open(self):
        """Mandatory V3 callback: triggers on successful wss handshake."""
        logger.info("🔌 V3 WebSocket Open — subscribing instruments...")
        try:
            self.streamer.subscribe(list(self.sub_list), "ltpc")
            self._consecutive_errors = 0 
            self._circuit_breaker_active = False
        except Exception as e:
            logger.error(f"V3 Open Subscription error: {e}")

    def _on_message(self, message):
        """
        V3 PROTOBUF HANDLER:
        The streamer automatically decodes binary protobuf into 'message'.
        """
        self.is_connected = True
        self.last_tick_time = time.time()
        self._reconnect_attempts = 0
        self._consecutive_errors = 0
        
        try:
            # V3 feed structure: feeds[instrument_key][data_type]
            feeds = getattr(message, 'feeds', {})
            for key, data in feeds.items():
                if hasattr(data, 'ltpc') and data.ltpc:
                    ltp = data.ltpc.ltp
                    if ltp:
                        self.rt_quotes[key] = float(ltp)
        except Exception as e:
            logger.debug(f"V3 Protobuf Parse error: {e}")

    def _on_error(self, error):
        self._consecutive_errors += 1
        logger.warning(f"V3 WS Error ({self._consecutive_errors}): {error}")
        self.is_connected = False
        
        if self._consecutive_errors >= self._max_consecutive_errors:
            logger.critical("❌ V3 Circuit Breaker Active - 30s Cooldown")
            self._circuit_breaker_active = True
            self._circuit_breaker_until = time.time() + 30
            self.disconnect()

    def _on_close(self, code, reason):
        logger.warning(f"V3 WS Closed (Code: {code}, Reason: {reason})")
        self.is_connected = False

    def _run_feed_process(self):
        """Main loop for V3 Streamer initialization."""
        try:
            config = upstox_client.Configuration()
            config.access_token = self.token
            api_client = upstox_client.ApiClient(config)
                
            # Initialize official V3 Streamer (handles Protobuf internally)
            self.streamer = MarketDataStreamerV3(api_client)
            self.streamer.on("open", self._on_open)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.on("close", self._on_close)
            
            logger.info("🔌 Initializing Upstox V3 Market Data Streamer...")
            self.streamer.connect()
        except Exception as e:
            logger.error(f"V3 Feed Process Crash: {e}")
            self._on_error(e)
        finally:
            self.is_connected = False
            self._thread_starting = False

    async def _ensure_thread_running(self):
        if self._circuit_breaker_active:
            if time.time() < self._circuit_breaker_until: return
            self._circuit_breaker_active = False

        if self.feed_thread and self.feed_thread.is_alive() and not self._thread_starting:
            return

        async with self._restart_lock:
            if self.feed_thread and self.feed_thread.is_alive(): return
            
            if self._reconnect_attempts > 0:
                backoff = min(2 ** self._reconnect_attempts, self._max_backoff)
                await asyncio.sleep(backoff)

            self._thread_starting = True
            self.disconnect()
            
            self.feed_thread = Thread(target=self._run_feed_process, daemon=True, name="UpstoxV3Feed")
            self.feed_thread.start()
            
            for _ in range(20):
                await asyncio.sleep(0.5)
                if self.is_connected:
                    self._reconnect_attempts = 0
                    break

    async def start(self):
        logger.info("🚀 V3 Supervisor Active")
        self.stop_event.clear()
        self.last_tick_time = time.time()
        
        while not self.stop_event.is_set():
            try:
                now = datetime.now(settings.IST).time()
                should_connect = (settings.MARKET_OPEN_TIME <= now <= settings.MARKET_CLOSE_TIME) or (settings.SAFETY_MODE != "live")
                
                if should_connect:
                    await self._ensure_thread_running()
                    # Heartbeat check
                    if time.time() - self.last_tick_time > 120 and self.is_connected:
                        logger.warning("⚠️ V3 Stalled - Reconnecting...")
                        self.disconnect()
                else:
                    if self.is_connected: self.disconnect()
                    await asyncio.sleep(60)
                    continue
                    
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Supervisor error: {e}")
                await asyncio.sleep(5)

    async def stop(self):
        self.stop_event.set()
        self.disconnect()

    def disconnect(self):
        try:
            if self.streamer:
                self.streamer.disconnect()
        except Exception:
            pass
        self.streamer = None
        self.is_connected = False
