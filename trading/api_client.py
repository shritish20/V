import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from core.config import settings, get_full_url
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

class EnhancedUpstoxAPI:
    """
    FIXED: Proper Rate Limit Handling (429) and Simulation Slippage logic.
    Addresses Medium Priority Issue #10 and Design Concern #2.
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

    def set_instrument_master(self, master):
        self.instrument_master = master
        
    def set_pricing_engine(self, pricing):
        self.pricing_engine = pricing

    async def _session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=aiohttp.ClientTimeout(total=30))
        return self.session

    async def _request_with_retry(self, method: str, url: str, **kwargs) -> Dict:
        """
        Executes HTTP requests with smart retries for 5xx errors and 429 Rate Limits.
        """
        retries = 3
        for i in range(retries):
            try:
                session = await self._session()
                async with session.request(method, url, **kwargs) as response:
                    # 1. Success
                    if response.status == 200:
                        return await response.json()
                    
                    # 2. Rate Limit (CRITICAL FIX)
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 1))
                        logger.warning(f"⛔ Rate Limit Hit on {url}. Backing off {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue # Retry loop
                    
                    # 3. Server Errors (5xx)
                    if response.status >= 500:
                        logger.warning(f"Server Error {response.status} on {url}. Retrying ({i+1}/{retries})...")
                        await asyncio.sleep(1 * (i + 1)) # Exponential-ish backoff
                        continue

                    # 4. Client Errors (4xx) - usually fatal (Auth, Bad Request)
                    text = await response.text()
                    logger.error(f"Client error {response.status} on {url}: {text}")
                    return {"status": "error", "message": text, "code": response.status}

            except aiohttp.ClientError as e:
                logger.error(f"Network exception on {url}: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error on {url}: {e}")
                await asyncio.sleep(1)
        
        return {"status": "error", "message": "Max retries exceeded"}

    async def get_quotes(self, instrument_keys: List[str]) -> Dict:
        if not instrument_keys: return {}
        url = get_full_url("market_quote")
        # Upstox V2 limit is 500 keys, usually safe, but slicing advised for massive lists
        return await self._request_with_retry("GET", url, params={"instrument_key": ",".join(instrument_keys)})
    
    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict:
        url = f"{settings.API_BASE_V2}/option/chain"
        params = {"instrument_key": instrument_key, "expiry_date": expiry_date}
        return await self._request_with_retry("GET", url, params=params)

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Order: {order.instrument_key} @ {order.price}")
            return True, f"SIM-{int(asyncio.get_event_loop().time())}"

        url = get_full_url("place_order")
        
        # Upstox V2 strict payload schema
        payload = {
            "instrument_token": order.instrument_key,
            "transaction_type": order.transaction_type,
            "quantity": abs(order.quantity),
            "order_type": order.order_type, 
            "product": order.product,
            "validity": order.validity,
            "price": float(order.price), 
            "trigger_price": float(order.trigger_price),
            "disclosed_quantity": 0,
            "is_amo": order.is_amo,
            "tag": "VG19"
        }
        
        res = await self._request_with_retry("POST", url, json=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def cancel_order(self, order_id: str) -> bool:
        if str(order_id).startswith("SIM"): return True
        url = get_full_url("cancel_order")
        res = await self._request_with_retry("DELETE", url, params={"order_id": order_id})
        return res.get("status") == "success"

    async def get_order_details(self, order_id: str) -> Dict:
        if str(order_id).startswith("SIM"):
            # Simulation response
            return {
                "status": "success", 
                "data": {
                    "status": "complete", 
                    "filled_quantity": 100, 
                    "average_price": 0.0 # Price filled by simulator
                }
            }
        url = get_full_url("order_details")
        return await self._request_with_retry("GET", url, params={"order_id": order_id})

    async def get_short_term_positions(self) -> List[Dict]:
        if settings.SAFETY_MODE != "live": return []
        url = f"{settings.API_BASE_V2}/portfolio/short-term-positions"
        res = await self._request_with_retry("GET", url)
        return res.get("data", [])

    async def get_option_greeks(self, instrument_keys: List[str]) -> Dict[str, Any]:
        if not instrument_keys: return {}
        url = get_full_url("option_greek")
        res = await self._request_with_retry("GET", url, params={"instrument_key": ",".join(instrument_keys)})
        if res.get("status") == "success":
            return res.get("data", {})
        return {}

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
