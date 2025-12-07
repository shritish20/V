import asyncio
import logging
from core.models import MultiLegTrade
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Simulating Batch Execution")
            trade.basket_order_id = f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"
            trade.status = "OPEN"
            trade.gtt_order_ids = [f"SIM-ORD-{i}" for i in range(len(trade.legs))]
            return True

        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            # FIX: Dynamic Slicing for Freeze Quantity
            # Nifty Freeze is typically 1800, BankNifty 900. 
            # Using 1800 as conservative default or check instrument master if available.
            needs_slicing = abs(leg.quantity) > 1800

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
                "slice": needs_slicing, # ✅ Dynamic
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

            # FIX: Robust Summary Parsing
            summary = response.get("data", [{}])[0].get("summary", {}) 
            # Note: Upstox structure varies. Sometimes summary is at root 'metadata'.
            # We check both locations to be safe.
            if not summary:
                summary = response.get("metadata", {}).get("summary", {})

            # Fallback: Calculate manually if summary missing
            data_list = response.get("data", [])
            success_ids = [x.get("order_id") for x in data_list if x.get("order_id")]
            errors = response.get("errors", [])

            if errors or len(success_ids) != len(trade.legs):
                logger.critical(f"❌ Batch Failure! Success: {len(success_ids)}/{len(trade.legs)}")
                
                # Rollback successful orders
                for oid in success_ids:
                    await self.api.cancel_order(oid)
                return False
            
            trade.gtt_order_ids = success_ids
            trade.basket_order_id = success_ids[0]
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
                # Parse Upstox Order Details Response
                order_data = details.get("data", {})
                status = order_data.get("status") if isinstance(order_data, dict) else ""
                
                if status != "complete":
                    all_filled = False
                    if status in ["cancelled", "rejected", "error"]:
                        logger.error(f"❌ Leg {oid} Failed: {status}")
                        return False 
            if all_filled: return True
            await asyncio.sleep(1)
            
        logger.warning(f"⚠️ Fill Verification Timeout {trade.id}")
        return False


