import asyncio
from typing import Optional
import aiohttp
from core.models import MultiLegTrade
from core.config import settings
from trading.margin_guard import MarginGuard
from utils.logger import get_logger
from database.manager import HybridDatabaseManager
from database.models import DbStrategy

logger = get_logger("OrderExecutor")

class LiveOrderExecutor:
    def __init__(self, db: HybridDatabaseManager):
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.base = settings.API_BASE_V2
        self.mg = MarginGuard()
        self.db = db

    async def place_multi_leg(self, trade: MultiLegTrade) -> Optional[str]:
        # Safety Hard-Lock
        if settings.SAFETY_MODE != "live":
            logger.warning(
                f"⚠️ SAFETY MODE {settings.SAFETY_MODE}: Skipping Live Order"
            )
            return f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"

        is_ok, _ = await self.mg.is_margin_ok(trade)
        if not is_ok:
            logger.warning("Margin insufficient – trade aborted")
            return None

        orders_payload = []
        for leg in trade.legs:
            orders_payload.append(
                {
                    "instrument_token": leg.instrument_key,
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "quantity": abs(leg.quantity),
                    "price": float(leg.entry_price),
                    "order_type": "LIMIT",
                    "product": "I",
                    "validity": "DAY",
                    "disclosed_quantity": 0,
                    "trigger_price": 0,
                    "is_amo": False,
                    "tag": f"VG19-{trade.id}",
                }
            )

        url = f"{self.base}/order/multi/place"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=orders_payload, headers=headers
                ) as resp:
                    data = await resp.json()
                    if resp.status == 200 and data.get("status") == "success":
                        order_ids = [o.get("order_id") for o in data.get("data", [])]
                        basket_id = order_ids[0] if order_ids else "UNKNOWN"
                        
                        if basket_id:
                            trade.basket_order_id = basket_id
                            trade.gtt_order_ids = order_ids
                            
                            async with self.db.get_session() as db_sess:
                                db_strat = await db_sess.get(DbStrategy, str(trade.id))
                                if db_strat:
                                    db_strat.broker_ref_id = basket_id
                                    await db_sess.merge(db_strat)
                        return basket_id
                    
                    logger.error(f"Multi-order failed: {data}")
                    return None
        except Exception as e:
            logger.error(f"Multi-order exception: {e}")
            return None
