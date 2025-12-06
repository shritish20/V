import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from core.config import settings, get_full_url
from core.models import GreeksSnapshot

logger = logging.getLogger("VolGuard18")

class EnhancedUpstoxAPI:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.session: Optional aiohttp.ClientSession = None
        self.pricing_engine = None
        self.subscribed_instruments: set = set()

    def set_pricing_engine(self, pricing_engine):
        self.pricing_engine = pricing_engine

    async def connect_ws(self, quote_callback):
        self.session = aiohttp.ClientSession()
        ws_url = f"{settings.WS_BASE_URL}?apiKey={settings.UPSTOX_ACCESS_TOKEN}"
        self.ws = await self.session.ws_connect(ws_url)
        logger.info("WebSocket connected")
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "quote":
                    instrument_key = data.get("instrument_key")
                    last_price = data.get("last_price")
                    if instrument_key and last_price:
                        await quote_callback(instrument_key, last_price)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error("WebSocket error")
                break

    async def subscribe_instruments(self, instrument_keys: List[str]):
        if not self.ws:
            logger.warning("WebSocket not connected")
            return
        new_keys = set(instrument_keys) - self.subscribed_instruments
        if new_keys:
            await self.ws.send_json({
                "action": "subscribe",
                "instrument_keys": list(new_keys)
            })
            self.subscribed_instruments.update(new_keys)
            logger.debug(f"Subscribed to {len(new_keys)} new instruments")

    async def get_quotes(self, instrument_keys: List[str]) -> Dict[str, Any]:
        url = get_full_url("market_quote")
        params = {"instrument_key": ",".join(instrument_keys)}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {})
                else:
                    logger.warning(f"Failed to fetch quotes: {resp.status}")
                    return {}

    async def fetch_option_chain(self, index_key: str, expiry: str) -> List[Dict[str, Any]]:
        url = get_full_url("option_chain")
        params = {"instrument_key": index_key, "expiry_date": expiry}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                else:
                    logger.warning(f"Failed to fetch option chain: {resp.status}")
                    return []

    async def get_available_expiries(self, index_key: str = settings.MARKET_KEY_INDEX) -> List[str]:
        chain = await self.fetch_option_chain(index_key, "")
        expiries = set()
        for item in chain:
            expiries.add(item.get("expiry_date", ""))
        return sorted(list(expiries))

    async def get_short_term_positions(self) -> List[Dict[str, Any]]:
        url = get_full_url("order_book")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return [p for p in data.get("data", []) if p.get("product") == "I" and p.get("net_quantity", 0) != 0]
                else:
                    logger.warning(f"Failed to fetch positions: {resp.status}")
                    return []

    async def place_order(self, order: Dict[str, Any]) -> Optional[str]:
        url = get_full_url("place_order")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, json=order) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {}).get("order_id")
                else:
                    logger.error(f"Order placement failed: {resp.status}")
                    return None

    async def place_gtt_order(self, gtt_order: Dict[str, Any]) -> Optional[str]:
        url = get_full_url("gtt_place")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, json=gtt_order) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {}).get("order_id")
                else:
                    logger.error(f"GTT order placement failed: {resp.status}")
                    return None

    async def cancel_order(self, order_id: str) -> bool:
        url = get_full_url("cancel_order")
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, headers=self.headers, params={"order_id": order_id}) as resp:
                if resp.status == 200:
                    logger.info(f"Order {order_id} cancelled")
                    return True
                else:
                    logger.warning(f"Failed to cancel order {order_id}: {resp.status}")
                    return False

    async def get_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        url = get_full_url("order_details")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params={"order_id": order_id}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {})
                else:
                    logger.warning(f"Failed to fetch order status: {resp.status}")
                    return None

    async def close(self):
        if self.session:
            await self.session.close()
