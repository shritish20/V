import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from core.config import settings, get_full_url
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

class EnhancedUpstoxAPI:
    """
    Schema-Verified Upstox API Client (VolGuard 19.0)
    Compatible with Upstox OpenAPI 3.1.0 definition.
    """
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Api-Version": "2.0",  # Keep 2.0 as base, specific V3 calls handle their own URLs
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
        retries = 3
        for i in range(retries):
            try:
                session = await self._session()
                async with session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        return await response.json()
                    
                    # [span_3](start_span)Rate Limit Handling (Schema: 429 Too Many Requests)[span_3](end_span)
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 1))
                        logger.warning(f"⛔ Rate Limit Hit on {url}. Backing off {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status >= 500:
                        logger.warning(f"Server Error {response.status}. Retrying...")
                        await asyncio.sleep(1 * (i + 1))
                        continue

                    text = await response.text()
                    logger.error(f"Client error {response.status} on {url}: {text}")
                    return {"status": "error", "message": text, "code": response.status}

            except Exception as e:
                logger.error(f"Request exception: {e}")
                await asyncio.sleep(1)
        return {"status": "error", "message": "Max retries exceeded"}

    async def get_quotes(self, instrument_keys: List[str]) -> Dict:
        """
        [span_4](start_span)Uses V3 Market Quote API[span_4](end_span)
        """
        if not instrument_keys: return {}
        # Schema confirms V3 endpoint for LTP/Full quotes
        url = "https://api-v2.upstox.com/v2/market-quote/quotes" # Fallback to V2 Full Quote for verified structure
        params = {"instrument_key": ",".join(instrument_keys)}
        return await self._request_with_retry("GET", url, params=params)
    
    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict:
        """
        [span_5](start_span)Schema: /v2/option/chain[span_5](end_span)
        """
        url = "https://api-v2.upstox.com/v2/option/chain"
        params = {"instrument_key": instrument_key, "expiry_date": expiry_date}
        return await self._request_with_retry("GET", url, params=params)

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        """
        [span_6](start_span)Schema: /v2/order/place[span_6](end_span) [span_7](start_span)or /v3/order/place[span_7](end_span)
        Using V2 for consistency with multi-order structure.
        """
        if settings.SAFETY_MODE != "live":
            return True, f"SIM-{int(asyncio.get_event_loop().time())}"

        url = "https://api-v2.upstox.com/v2/order/place"
        
        # [span_8](start_span)Schema-verified payload keys[span_8](end_span)
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
        """
        [span_9](start_span)Schema: /v2/order/multi/place[span_9](end_span)
        Required for Atomic Batch Execution.
        """
        url = "https://api-v2.upstox.com/v2/order/multi/place"
        return await self._request_with_retry("POST", url, json=orders_payload)

    async def cancel_order(self, order_id: str) -> bool:
        if str(order_id).startswith("SIM"): return True
        [span_10](start_span)url = "https://api-v2.upstox.com/v2/order/cancel" #[span_10](end_span)
        res = await self._request_with_retry("DELETE", url, params={"order_id": order_id})
        return res.get("status") == "success"

    async def get_order_details(self, order_id: str) -> Dict:
        if str(order_id).startswith("SIM"):
             return {"status": "success", "data": {"status": "complete", "average_price": 0.0}}
        
        # [span_11](start_span)Schema: /v2/order/history[span_11](end_span) gives details by order_id
        url = "https://api-v2.upstox.com/v2/order/history"
        return await self._request_with_retry("GET", url, params={"order_id": order_id})

    async def get_funds(self) -> Dict:
        """
        [span_12](start_span)Schema: /v2/user/get-funds-and-margin[span_12](end_span)
        """
        url = "https://api-v2.upstox.com/v2/user/get-funds-and-margin"
        params = {"segment": "SEC"}
        res = await self._request_with_retry("GET", url, params=params)
        if res.get("status") == "success":
            return res.get("data", {}).get("equity", {})
        return {}
    
    async def get_margin(self, instruments_payload: List[Dict]) -> Dict:
        """
        [span_13](start_span)Schema: /v2/charges/margin[span_13](end_span)
        """
        url = "https://api-v2.upstox.com/v2/charges/margin"
        res = await self._request_with_retry("POST", url, json={"instruments": instruments_payload})
        return res

    async def get_option_greeks(self, instrument_keys: List[str]) -> Dict[str, Any]:
        """
        [span_14](start_span)Schema: /v3/market-quote/option-greek[span_14](end_span)
        """
        if not instrument_keys: return {}
        url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        res = await self._request_with_retry("GET", url, params={"instrument_key": ",".join(instrument_keys)})
        if res.get("status") == "success":
            return res.get("data", {})
        return {}

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
