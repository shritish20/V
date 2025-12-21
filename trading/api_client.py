import aiohttp
import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote  # <--- CRITICAL IMPORT
from core.config import settings, UPSTOX_API_ENDPOINTS
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

class TokenExpiredError(Exception):
    pass

class RateLimiter:
    """Token Bucket Rate Limiter"""
    def __init__(self, rate_limit_per_sec=10):
        self.rate = rate_limit_per_sec
        self.tokens = rate_limit_per_sec
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

class EnhancedUpstoxAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Api-Version": "2.0"
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self.instrument_master = None
        
        # Rate Limiter
        self.limiter = RateLimiter(rate_limit_per_sec=9)

    def set_instrument_master(self, master):
        self.instrument_master = master

    async def update_token(self, new_token: str):
        async with self._session_lock:
            self.token = new_token
            self.headers["Authorization"] = f"Bearer {new_token}"
            if self.session:
                await self.session.close()
                self.session = None
            logger.info("🔄 Token Rotated")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers)
        return self.session

    async def _request_with_retry(self, method: str, endpoint_key: str, dynamic_url: str = None, **kwargs) -> Dict:
        if dynamic_url:
            url = dynamic_url
        else:
            path = UPSTOX_API_ENDPOINTS.get(endpoint_key, "")
            url = f"{settings.API_BASE_URL}{path}"
            
        for i in range(3):
            try:
                await self.limiter.wait()
                
                session = await self._get_session()
                async with session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        return await response.json()
                    if response.status == 401:
                        raise TokenExpiredError("Token Invalid")
                    if response.status == 429:
                        logger.warning("⚠️ Rate Limit Hit (429). Backing off...")
                        await asyncio.sleep(2)
                        continue
                    
                    text = await response.text()
                    # Don't log error for 423 (Maintenance) as it's handled by MarginGuard
                    if response.status != 423:
                        logger.error(f"API Error {response.status}: {text}")
                    return {"status": "error", "message": text, "code": response.status}
            except TokenExpiredError:
                raise
            except Exception as e:
                logger.error(f"Req Failed: {e}")
                await asyncio.sleep(1)
        return {"status": "error"}

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            return True, f"SIM-{int(asyncio.get_event_loop().time())}"
        
        payload = {
            "quantity": abs(order.quantity),
            "product": order.product,
            "validity": order.validity,
            "price": float(order.price),
            "tag": "VG19",
            "instrument_token": order.instrument_key,
            "order_type": order.order_type,
            "transaction_type": order.transaction_type,
            "disclosed_quantity": 0,
            "trigger_price": float(order.trigger_price),
            "is_amo": order.is_amo
        }
        res = await self._request_with_retry("POST", "place_order", json=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def place_multi_order(self, orders_payload: List[Dict]) -> Dict:
        if settings.SAFETY_MODE != "live":
            return {"status": "success", "data": [{"order_id": f"SIM-BATCH-{i}"} for i in range(len(orders_payload))]}
        return await self._request_with_retry("POST", "place_multi_order", json=orders_payload)

    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict:
        return await self._request_with_retry("GET", "option_chain", params={
            "instrument_key": instrument_key, "expiry_date": expiry_date
        })

    async def get_short_term_positions(self) -> List[Dict]:
        res = await self._request_with_retry("GET", "positions")
        return res.get("data", []) if res.get("status") == "success" else []

    async def get_holidays(self) -> Dict:
        return await self._request_with_retry("GET", "holidays")

    # --- FIXED URL ENCODING HERE ---
    async def get_historical_candles(self, instrument_key: str, interval: str, to_date: str, from_date: str) -> Dict:
        """
        Fetches historical candles.
        CRITICAL FIX: URL Encodes the instrument_key (e.g. 'NSE_INDEX|Nifty 50' -> 'NSE_INDEX%7CNifty%2050')
        """
        encoded_key = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v3/historical-candle/{encoded_key}/{interval}/{to_date}/{from_date}"
        return await self._request_with_retry("GET", "history_v3", dynamic_url=url)

    async def get_intraday_candles(self, instrument_key: str, interval: str) -> Dict:
        encoded_key = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v3/historical-candle/intraday/{encoded_key}/{interval}"
        return await self._request_with_retry("GET", "intraday_v3", dynamic_url=url)

    async def get_market_quote_ohlc(self, instrument_key: str, interval: str) -> Dict:
        return await self._request_with_retry(
            "GET", 
            "market_quote_ohlc", 
            params={"instrument_key": instrument_key, "interval": interval}
        )

    async def close(self):
        async with self._session_lock:
            if self.session: await self.session.close()
