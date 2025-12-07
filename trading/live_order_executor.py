import asyncio
import logging
from typing import Optional, List, Dict
from core.models import MultiLegTrade
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    """
    Schema-Verified Order Executor.
    Parses /v2/order/multi/place response correctly.
    """
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Batch Execution")
            trade.basket_order_id = f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"
            trade.status = "OPEN"
            trade.gtt_order_ids = [f"SIM-ORD-{i}" for i in range(len(trade.legs))]
            return True

        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            # [span_16](start_span)Schema: MultiOrderRequest[span_16](end_span)
            order = {
                "instrument_token": leg.instrument_key,
                "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                "quantity": abs(leg.quantity),
                "product": "I", # Intraday
                "validity": "DAY",
                "order_type": "MARKET" if leg.entry_price <= 0 else "LIMIT",
                "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "slice": False, # Explicitly false unless handling large orders
                [span_17](start_span)"correlation_id": f"LEG{idx}-{trade.id[:10]}", # Unique ID required[span_17](end_span)
                "tag": "VG19"
            }
            orders_payload.append(order)

        try:
            # Uses the schema-compliant multi-order method
            response = await self.api.place_multi_order(orders_payload)
            
            if response.get("status") != "success":
                logger.error(f"❌ Batch API Failed: {response}")
                return False

            # [span_18](start_span)Schema: MultiOrderResponse[span_18](end_span)
            # Data is a list of objects with 'order_id' or errors
            data_list = response.get("data", [])
            errors = response.get("errors", [])

            success_ids = []
            for item in data_list:
                if "order_id" in item:
                    success_ids.append(item["order_id"])

            if errors or len(success_ids) != len(trade.legs):
                logger.critical(f"❌ Batch Failure! Success: {len(success_ids)}/{len(trade.legs)}")
                # Rollback partials
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

        delays = [0.5, 1.0, 2.0, 3.0, 5.0, 5.0, 5.0, 5.0] # Backoff
        start_time = asyncio.get_event_loop().time()
        
        for delay in delays:
            if (asyncio.get_event_loop().time() - start_time) > timeout:
                break
                
            all_filled = True
            for oid in trade.gtt_order_ids:
                try:
                    # [span_19](start_span)Schema: /v2/order/history returns array[span_19](end_span)
                    details = await self.api.get_order_details(oid)
                    data = details.get("data", [])
                    
                    if not data:
                        all_filled = False
                        break

                    # The API returns history; latest status is usually the first item or check 'status' field
                    # Upstox history array: item 0 is usually latest
                    latest_status = data[0].get("status", "").lower()
                    
                    if latest_status == "complete":
                        continue
                    elif latest_status in ["cancelled", "rejected", "error"]:
                        logger.error(f"❌ Leg {oid} Failed: {latest_status}")
                        return False
                    else:
                        all_filled = False
                        break 
                except Exception:
                    all_filled = False
            
            if all_filled:
                return True
            await asyncio.sleep(delay)
            
        logger.warning(f"⚠️ Fill Verification Timeout {trade.id}")
        return False
