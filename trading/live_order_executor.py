import asyncio
import logging
from typing import Optional, List, Dict
from core.models import MultiLegTrade
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        """
        Executes atomic batch. Returns True only if ALL legs accepted.
        """
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Simulating Batch Execution")
            trade.basket_order_id = f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"
            trade.status = "OPEN" # Temporarily set status
            # Generate fake order IDs for sim
            trade.gtt_order_ids = [f"SIM-ORD-{i}" for i in range(len(trade.legs))]
            return True

        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            order = {
                "instrument_token": leg.instrument_key,
                "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                "quantity": abs(leg.quantity),
                "product": "I", 
                "validity": "DAY",
                "order_type": "MARKET" if leg.entry_price <= 0 else "LIMIT",
                "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "slice": False,
                "correlation_id": f"{trade.id}-LEG{idx}", 
                "tag": "VG19"
            }
            orders_payload.append(order)

        try:
            url = f"{settings.API_BASE_V2}/order/multi/place"
            response = await self.api._request_with_retry("POST", url, json=orders_payload)
            
            if response.get("status") != "success":
                logger.error(f"❌ Batch API Failed: {response}")
                return False

            # FIX: Check Summary for Errors
            summary = response.get("data", [{}])[0].get("summary", {}) # Upstox response structure can vary, handle carefully
            # Actually, typically response['data'] is a list of order results.
            # But sometimes there's a summary field in the root or metadata.
            # Let's rely on iterating individual statuses.
            
            data_list = response.get("data", [])
            order_ids = []
            has_error = False
            
            for item in data_list:
                if item.get("error_code") or item.get("status") == "error":
                    has_error = True
                    logger.error(f"Leg Error: {item}")
                elif item.get("order_id"):
                    order_ids.append(item.get("order_id"))

            if has_error or len(order_ids) != len(trade.legs):
                logger.critical(f"❌ Partial Batch Failure! Expected {len(trade.legs)}, got {len(order_ids)} success.")
                # Emergency: Cancel the ones that succeeded
                for oid in order_ids:
                    await self.api.cancel_order(oid)
                return False
            
            trade.gtt_order_ids = order_ids
            trade.basket_order_id = order_ids[0]
            logger.info(f"✅ Batch Accepted. Ref: {trade.basket_order_id}")
            return True

        except Exception as e:
            logger.error(f"❌ Batch Exception: {e}")
            return False

    async def verify_fills(self, trade: MultiLegTrade, timeout=30) -> bool:
        if settings.SAFETY_MODE != "live": return True

        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_filled = True
            for oid in trade.gtt_order_ids:
                details = await self.api.get_order_details(oid)
                status = details.get("data", {}).get("status")
                if status != "complete":
                    all_filled = False
                    if status in ["cancelled", "rejected", "error"]:
                        logger.error(f"❌ Leg {oid} Failed: {status}")
                        return False 
            if all_filled: return True
            await asyncio.sleep(1)
            
        logger.warning(f"⚠️ Fill Verification Timeout {trade.id}")
        return False

