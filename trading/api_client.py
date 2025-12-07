import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from core.config import settings, get_full_url
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

class EnhancedUpstoxAPI:
    """
    PRODUCTION-READY: Thread-Safe Token Rotation + Session Management
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
        
        # CRITICAL FIX: Session lock prevents race conditions
        self._session_lock = asyncio.Lock()

    def set_instrument_master(self, master):
        self.instrument_master = master
        
    def set_pricing_engine(self, pricing):
        self.pricing_engine = pricing

    async def update_token(self, new_token: str):
        """
        PRODUCTION FIX: Thread-safe token rotation.
        Call this from /api/token/refresh endpoint.
        """
        if new_token == self.token:
            return
        
        async with self._session_lock:
            logger.info("🔐 Updating API token (thread-safe)")
            self.token = new_token
            self.headers["Authorization"] = f"Bearer {new_token}"
            
            # Force session close to rebuild with new headers
            if self.session and not self.session.closed:
                await self.session.close()
            self.session = None
            
            logger.info("✅ Token updated successfully")

    async def _session(self) -> aiohttp.ClientSession:
        """
        PRODUCTION FIX: Thread-safe session creation with lock.
        """
        async with self._session_lock:
            if self.session is None or self.session.closed:
                # Rebuild session with current headers
                self.session = aiohttp.ClientSession(
                    headers=self.headers.copy(),  # Use copy to prevent mutation
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
                    
                    # Rate Limit Handling (429)
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 1))
                        logger.warning(f"⛔ Rate Limit Hit. Backing off {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    # Server Errors (5xx)
                    if response.status >= 500:
                        logger.warning(f"Server Error {response.status}. Retrying...")
                        await asyncio.sleep(1 * (i + 1))
                        continue

                    # Client Errors (4xx)
                    text = await response.text()
                    
                    # PRODUCTION FIX: Handle 401 specially
                    if response.status == 401:
                        logger.error(
                            f"❌ 401 Unauthorized on {url}. "
                            f"Token may be expired or invalid."
                        )
                    else:
                        logger.error(f"Client error {response.status} on {url}: {text}")
                    
                    return {
                        "status": "error", 
                        "message": text, 
                        "code": response.status
                    }

            except asyncio.TimeoutError:
                logger.warning(f"Request timeout on {url} (attempt {i+1}/{retries})")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Request exception: {e}")
                await asyncio.sleep(1)
        
        return {"status": "error", "message": "Max retries exceeded"}

    async def get_quotes(self, instrument_keys: List[str]) -> Dict:
        """Uses V2 Market Quote API"""
        if not instrument_keys: 
            return {}
        url = "https://api-v2.upstox.com/v2/market-quote/quotes"
        params = {"instrument_key": ",".join(instrument_keys)}
        return await self._request_with_retry("GET", url, params=params)
    
    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict:
        """Schema: /v2/option/chain"""
        url = "https://api-v2.upstox.com/v2/option/chain"
        params = {"instrument_key": instrument_key, "expiry_date": expiry_date}
        return await self._request_with_retry("GET", url, params=params)

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        """Schema: /v2/order/place"""
        if settings.SAFETY_MODE != "live":
            return True, f"SIM-{int(asyncio.get_event_loop().time())}"

        url = "https://api-v2.upstox.com/v2/order/place"
        
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
        
        res = await self._request_with_retry("POST", url, json=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def place_multi_order(self, orders_payload: List[Dict]) -> Dict:
        """Schema: /v2/order/multi/place (Atomic Batch)"""
        url = "https://api-v2.upstox.com/v2/order/multi/place"
        return await self._request_with_retry("POST", url, json=orders_payload)

    async def cancel_order(self, order_id: str) -> bool:
        if str(order_id).startswith("SIM"): 
            return True
        url = "https://api-v2.upstox.com/v2/order/cancel"
        res = await self._request_with_retry("DELETE", url, params={"order_id": order_id})
        return res.get("status") == "success"

    async def get_order_details(self, order_id: str) -> Dict:
        if str(order_id).startswith("SIM"):
            return {
                "status": "success", 
                "data": [{"status": "complete", "average_price": 0.0}]
            }
        
        url = "https://api-v2.upstox.com/v2/order/history"
        return await self._request_with_retry("GET", url, params={"order_id": order_id})

    async def get_funds(self) -> Dict:
        """Schema: /v2/user/get-funds-and-margin"""
        url = "https://api-v2.upstox.com/v2/user/get-funds-and-margin"
        params = {"segment": "SEC"}
        res = await self._request_with_retry("GET", url, params=params)
        if res.get("status") == "success":
            return res.get("data", {}).get("equity", {})
        return {}
    
    async def get_margin(self, instruments_payload: List[Dict]) -> Dict:
        """Schema: /v2/charges/margin"""
        url = "https://api-v2.upstox.com/v2/charges/margin"
        res = await self._request_with_retry("POST", url, json={"instruments": instruments_payload})
        return res

    async def get_option_greeks(self, instrument_keys: List[str]) -> Dict[str, Any]:
        """Schema: /v3/market-quote/option-greek"""
        if not instrument_keys: 
            return {}
        url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        res = await self._request_with_retry("GET", url, params={"instrument_key": ",".join(instrument_keys)})
        if res.get("status") == "success":
            return res.get("data", {})
        return {}

    async def get_short_term_positions(self) -> List[Dict]:
        """Schema: /v2/portfolio/short-term-positions"""
        url = "https://api-v2.upstox.com/v2/portfolio/short-term-positions"
        res = await self._request_with_retry("GET", url)
        if res.get("status") == "success":
            return res.get("data", [])
        return []

    async def close(self):
        """Cleanup: Close session gracefully"""
        async with self._session_lock:
            if self.session and not self.session.closed:
                await self.session.close()
                logger.info("✅ API session closed")
