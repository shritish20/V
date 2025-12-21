import asyncio
import logging
from typing import List, Dict
import time
from core.models import MultiLegTrade
from core.config import settings

logger = logging.getLogger("LiveExecutor")

class LiveOrderExecutor:
    def __init__(self, api_client):
        self.api = api_client

    async def place_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        payload = []
        for leg in trade.legs:
            qty = abs(leg.quantity)
            max_qty = settings.NIFTY_FREEZE_QTY
            
            slices = [max_qty] * (qty // max_qty) + [qty % max_qty]
            slices = [s for s in slices if s > 0]
            
            for s in slices:
                payload.append({
                    "quantity": s,
                    "product": "I",
                    "validity": "DAY",
                    "price": 0.0,
                    "tag": trade.id,
                    "instrument_token": leg.instrument_key,
                    "order_type": "MARKET",
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "disclosed_quantity": 0,
                    "trigger_price": 0.0,
                    "is_amo": False,
                    "slice": False,
                    "correlation_id": f"{trade.id}-{int(time.time()*1000)}"
                })

        try:
            logger.info(f"🚀 Sending Batch: {len(payload)} orders")
            res = await self.api.place_multi_order(payload)
            
            if res.get("status") == "success":
                logger.info(f"✅ Batch Executed: {trade.id}")
                return True
            else:
                logger.error(f"❌ Batch Failed: {res}")
                return False
                
        except Exception as e:
            logger.error(f"Executor Exception: {e}")
            return False
