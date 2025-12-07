import aiohttp
import asyncio
import logging
import calendar
from datetime import datetime, date
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
        self.instrument_master = None  # Will be set by engine

    def set_instrument_master(self, master):
        """Link the InstrumentMaster for accurate symbol resolution"""
        self.instrument_master = master

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
                        text = await response.text()
                        logger.error(f"Client error {response.status} on {url}: {text}")
                        return {"status": "error", "message": text}
                    if response.status >= 500:
                        logger.warning(f"Server error {response.status} on {url}, retry {i+1}/{retries}")
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
        
        # FINAL SAFETY CHECK: Ensure we aren't sending a raw symbol like "NIFTY..."
        if "|" not in order.instrument_key:
            logger.critical(f"🛑 BLOCKED: Attempted to place order with invalid key: {order.instrument_key}")
            return False, None

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

    # --- RESOLUTION LOGIC (The Fixed Part) ---

    async def get_current_future_symbol(self, index_key: str = "NSE_INDEX|Nifty 50") -> str:
        """Returns the real instrument key for current month futures."""
        if self.instrument_master:
            # Use Master (Accurate)
            token = self.instrument_master.get_current_future("NIFTY")
            if token: return token
            
        # Fallback (heuristic)
        now = datetime.now()
        yy = str(now.year)[-2:]
        mmm = calendar.month_abbr[now.month].upper()
        fallback = f"NSE_FO|NIFTY{yy}{mmm}FUT"
        logger.warning(f"⚠️ Using fallback Future symbol: {fallback}")
        return fallback

    async def resolve_instrument_key(self, strike: float, type: str, expiry: str) -> str:
        """
        Resolves NIFTY 21000 CE 2024-12-28 -> NSE_FO|12345
        """
        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
        
        # 1. Try Master File (Primary Method)
        if self.instrument_master:
            token = self.instrument_master.get_option_token("NIFTY", strike, type, expiry_date)
            if token:
                return token
                
        # 2. Fallback (Only if master fails or is missing)
        # Warning: This heuristic often fails for Weekly options
        logger.warning(f"⚠️ Master lookup failed for {strike} {type} {expiry}. Using heuristic.")
        
        yy = str(expiry_date.year)[-2:]
        mmm = expiry_date.strftime("%b").upper()
        day = expiry_date.day
        
        # Upstox Weekly format often looks like '24D07' (Year + MonthChar + Day)
        # But for safety, we default to the standard monthly format here.
        # This is why having InstrumentMaster loaded is crucial.
        return f"NSE_FO|NIFTY{yy}{mmm}{int(strike)}{type}"

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
