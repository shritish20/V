import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from core.config import settings, get_full_url
from core.models import Order
from trading.instrument_master import InstrumentMaster

logger = logging.getLogger("UpstoxAPI")

class EnhancedUpstoxAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Api-Version": "2.0",
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.master = InstrumentMaster()

    async def initialize(self):
        if not await self.validate_token():
            raise RuntimeError("❌ Invalid or Expired Upstox Access Token")
        await self.master.download_and_load()

    async def _session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session

    async def validate_token(self) -> bool:
        url = get_full_url("user_profile")
        try:
            session = await self._session()
            async with session.get(url) as response:
                if response.status == 200:
                    logger.info("✅ Token Validated")
                    return True
                logger.critical(f"⛔ Token Invalid! HTTP {response.status}")
                return False
        except Exception as e:
            logger.critical(f"Token validation failed: {e}")
            return False

    async def _request_with_retry(self, method: str, url: str, **kwargs) -> Dict:
        retries = 3
        for i in range(retries):
            try:
                session = await self._session()
                async with session.request(method, url, **kwargs) as response:
                    if 400 <= response.status < 500:
                        logger.error(f"Client error {response.status} on {url}: {await response.text()}")
                        return {}
                    if response.status >= 500:
                        await asyncio.sleep(1)
                        continue
                    return await response.json()
            except Exception as e:
                logger.error(f"Request exception on {url}: {e}")
                await asyncio.sleep(1)
        return {}

    async def get_current_future_symbol(self, index_symbol: str = "NIFTY") -> str:
        search_sym = "NIFTY" if "Nifty 50" in index_symbol else "BANKNIFTY"
        token = self.master.get_current_future(search_sym)
        if not token:
            logger.error(f"Could not resolve future for {index_symbol}")
            return "NSE_FO|00000"
        return token

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            await asyncio.sleep(0.1)
            logger.info(f"[PAPER] Order Placed: {order.transaction_type} {order.quantity}")
            return True, f"SIM-{int(asyncio.get_event_loop().time())}"

        url = get_full_url("place_order")
        payload = {
            "instrument_token": order.instrument_key,
            "transaction_type": order.transaction_type,
            "quantity": abs(order.quantity),
            "order_type": order.order_type.value,
            "price": round(order.price, 2),
            "product": order.product,
            "validity": order.validity,
            "disclosed_quantity": 0,
            "trigger_price": round(order.trigger_price, 2),
            "is_amo": False,
            "tag": "VG19",
        }
        res = await self._request_with_retry("POST", url, json=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("SIM"): return True
        url = get_full_url("cancel_order")
        res = await self._request_with_retry("DELETE", url, params={"order_id": order_id})
        return res.get("status") == "success"

    async def get_order_details(self, order_id: str) -> Dict:
        if order_id.startswith("SIM"):
            return {"status": "complete", "filled_quantity": 100, "average_price": 100.0}
        url = get_full_url("order_details")
        return await self._request_with_retry("GET", url, params={"order_id": order_id})

    async def get_short_term_positions(self) -> List[Dict]:
        if settings.SAFETY_MODE != "live": return []
        url = f"{settings.API_BASE_V2}/portfolio/short-term-positions"
        res = await self._request_with_retry("GET", url)
        return res.get("data", []) or []

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
