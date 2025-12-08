import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from core.config import settings
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

class TokenExpiredError(Exception):
    """Custom exception to signal immediate token refresh"""
    pass

class EnhancedUpstoxAPI:
    """
    Production-Ready API Client.
    Features: Thread-Safe Token Rotation, Retry Logic, and Session Management.
    """
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Api-Version": "2.0",
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.instrument_master = None
        self.pricing_engine = None
        self._session_lock = asyncio.Lock()

    def set_instrument_master(self, master):
        self.instrument_master = master

    def set_pricing_engine(self, pricing):
        self.pricing_engine = pricing

    async def update_token(self, new_token: str):
        """FIXED: Thread-safe token rotation that prevents Deadlocks."""
        if new_token == self.token:
            return

        old_session = None
        # Acquire Lock ONLY to swap credentials and detach session
        async with self._session_lock:
            logger.info("🔐 Updating API token...")
            self.token = new_token
            self.headers["Authorization"] = f"Bearer {new_token}"
            
            # Detach the old session if it exists
            if self.session and not self.session.closed:
                old_session = self.session
                self.session = None

        # Close the old session OUTSIDE the lock to prevent freezing
        if old_session:
            await old_session.close()
        logger.info("✅ Token rotation complete.")

    async def _session(self) -> aiohttp.ClientSession:
        """Get or create the session thread-safely."""
        async with self._session_lock:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession(
                    headers=self.headers.copy(),
                    timeout=aiohttp.ClientTimeout(total=30)
                )
            return self.session

    async def _request_with_retry(self, method: str, url: str, **kwargs) -> Dict:
        retries = 3
        for i in range(retries):
            try:
                session = await self._session()
                async with session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        return await response.json()
                    
                    # Rate Limit (429)
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 1))
                        logger.warning(f"⛔ Rate Limit. Backing off {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    
                    # CRITICAL FIX: Auth Error (401)
                    if response.status == 401:
                        logger.critical(f"❌ 401 Unauthorized on {url}")
                        # Raise exception so Engine can catch it and trigger refresh
                        raise TokenExpiredError("Access Token Invalid or Expired")
                    
                    # Server Error (5xx)
                    if response.status >= 500:
                        logger.warning(f"Server Error {response.status}. Retrying...")
                        await asyncio.sleep(1 * (i + 1))
                        continue
                        
                    # Client Error (4xx) - usually fatal, don't retry
                    text = await response.text()
                    logger.error(f"Client error {response.status} on {url}: {text}")
                    return {"status": "error", "message": text, "code": response.status}

            except TokenExpiredError:
                raise  # Propagate up
            except asyncio.TimeoutError:
                logger.warning(f"Request timeout on {url} (attempt {i+1}/{retries})")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Request exception: {e}")
                await asyncio.sleep(1)
        
        return {"status": "error", "message": "Max retries exceeded"}

    async def get_quotes(self, instrument_keys: List[str]) -> Dict:
        if not instrument_keys: return {}
        url = "https://api-v2.upstox.com/v2/market-quote/quotes"
        return await self._request_with_retry("GET", url, params={"instrument_key": ",".join(instrument_keys)})

    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict:
        url = "https://api-v2.upstox.com/v2/option/chain"
        return await self._request_with_retry("GET", url, params={"instrument_key": instrument_key, "expiry_date": expiry_date})

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            return True, f"SIM-{int(asyncio.get_event_loop().time())}"
        
        url = "https://api-v2.upstox.com/v2/order/place"
        payload = {
            "quantity": abs(order.quantity), "product": order.product,
            "validity": order.validity, "price": float(order.price),
            "tag": "VG19", "instrument_token": order.instrument_key,
            "order_type": order.order_type, "transaction_type": order.transaction_type,
            "disclosed_quantity": 0, "trigger_price": float(order.trigger_price),
            "is_amo": order.is_amo
        }
        res = await self._request_with_retry("POST", url, json=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def place_multi_order(self, orders_payload: List[Dict]) -> Dict:
        url = "https://api-v2.upstox.com/v2/order/multi/place"
        return await self._request_with_retry("POST", url, json=orders_payload)

    async def cancel_order(self, order_id: str) -> bool:
        if str(order_id).startswith("SIM"): return True
        url = "https://api-v2.upstox.com/v2/order/cancel"
        res = await self._request_with_retry("DELETE", url, params={"order_id": order_id})
        return res.get("status") == "success"

    async def get_order_details(self, order_id: str) -> Dict:
        if str(order_id).startswith("SIM"):
            return {"status": "success", "data": [{"status": "complete", "average_price": 0.0}]}
        
        # NOTE: Using v2 order history/details endpoint
        url = "https://api-v2.upstox.com/v2/order/details"
        return await self._request_with_retry("GET", url, params={"order_id": order_id})

    async def get_funds(self) -> Dict:
        url = "https://api-v2.upstox.com/v2/user/get-funds-and-margin"
        res = await self._request_with_retry("GET", url, params={"segment": "SEC"})
        if res.get("status") == "success":
            return res.get("data", {}).get("equity", {})
        return {}

    async def get_margin(self, instruments_payload: List[Dict]) -> Dict:
        url = "https://api-v2.upstox.com/v2/charges/margin"
        return await self._request_with_retry("POST", url, json={"instruments": instruments_payload})

    async def get_option_greeks(self, instrument_keys: List[str]) -> Dict[str, Any]:
        if not instrument_keys: return {}
        url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        res = await self._request_with_retry("GET", url, params={"instrument_key": ",".join(instrument_keys)})
        return res.get("data", {}) if res.get("status") == "success" else {}

    async def get_short_term_positions(self) -> List[Dict]:
        url = "https://api-v2.upstox.com/v2/portfolio/short-term-positions"
        res = await self._request_with_retry("GET", url)
        return res.get("data", []) if res.get("status") == "success" else []

    async def close(self):
        """Cleanup: Close session gracefully"""
        async with self._session_lock:
            if self.session and not self.session.closed:
                await self.session.close()
        logger.info("✅ API session closed")
