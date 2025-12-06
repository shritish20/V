import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from core.config import settings, get_full_url
from core.models import Order

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

    async def _session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session

    async def _request_with_retry(self, method: str, url: str, **kwargs) -> Dict:
        retries = 3
        for i in range(retries):
            try:
                session = await self._session()
                async with session.request(method, url, **kwargs) as response:
                    if 400 <= response.status < 500:
                        logger.error(
                            f"Client error {response.status} on {url}: {await response.text()}"
                        )
                        return {}
                    if response.status >= 500:
                        logger.warning(
                            f"Server error {response.status} on {url}, retry {i+1}/{retries}"
                        )
                        await asyncio.sleep(1)
                        continue
                    return await response.json()
            except Exception as e:
                logger.error(f"Request exception on {url}: {e}")
                await asyncio.sleep(1)
        return {}

    async def get_quotes(self, instrument_keys: List[str]) -> Dict:
        if not instrument_keys:
            return {}
        url = get_full_url("market_quote")
        return await self._request_with_retry(
            "GET", url, params={"instrument_key": ",".join(instrument_keys)}
        )

    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            await asyncio.sleep(0.1)
            logger.info(
                f"[{settings.SAFETY_MODE}] Order Sim: {order.transaction_type} "
                f"{order.quantity} of {order.instrument_key}"
            )
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
            "disclosed_quantity": order.disclosed_quantity,
            "trigger_price": round(order.trigger_price, 2),
            "is_amo": False,
            "tag": "VG19",
        }
        res = await self._request_with_retry("POST", url, json=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        logger.error(f"Order Failed: {res}")
        return False, None

    async def cancel_order(self, order_id: str) -> bool:
        if order_id.startswith("SIM"):
            return True
        url = get_full_url("cancel_order")
        res = await self._request_with_retry(
            "DELETE", url, params={"order_id": order_id}
        )
        return res.get("status") == "success"

    async def get_order_details(self, order_id: str) -> Dict:
        if order_id.startswith("SIM"):
            return {
                "status": "complete",
                "filled_quantity": 100,
                "average_price": 100.0,
            }
        url = get_full_url("order_details")
        return await self._request_with_retry(
            "GET", url, params={"order_id": order_id}
        )

    async def get_short_term_positions(self) -> List[Dict]:
        if settings.SAFETY_MODE != "live":
            return []
        url = f"{settings.API_BASE_V2}/portfolio/short-term-positions"
        res = await self._request_with_retry("GET", url)
        return res.get("data", [])

    async def get_option_greeks(self, instrument_keys: List[str]) -> Dict[str, Any]:
        if not instrument_keys:
            return {}
        url = get_full_url("option_greek")
        params = {"instrument_key": ",".join(instrument_keys)}
        try:
            response = await self._request_with_retry("GET", url, params=params)
            if response.get("status") == "success":
                return response.get("data", {})
        except Exception as e:
            logger.error(f"Failed to fetch Upstox Greeks: {e}")
        return {}

    async def get_current_future_symbol(self, index_key: str) -> str:
        # NOTE: Implement proper resolution via instruments dump in future
        return "NSE_FO|NIFTY24DECFUT"

    async def resolve_instrument_key(self, strike: float, type: str, expiry: str) -> str:
        # Placeholder: In production this should query a local instrument master DB
        # Format: NSE_FO|NIFTY23DEC21000CE
        from datetime import datetime
        formatted_date = datetime.strptime(expiry, "%Y-%m-%d").strftime("%y%b").upper()
        return f"NSE_FO|NIFTY{formatted_date}{int(strike)}{type}"

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
