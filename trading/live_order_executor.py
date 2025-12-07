import asyncio
import logging
from typing import Optional, List, Dict
from core.models import MultiLegTrade
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    """
    FIXED: Implemented exponential backoff for order status polling.
    Addresses High Priority Issue #6: "Order Status Polling Inefficiency"
    """
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
            # Auto-slice large orders (>1800 qty for Nifty is common limit)
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
                "slice": needs_slicing,
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

            summary = response.get("data", [{}])[0].get("summary", {}) 
            if not summary:
                summary = response.get("metadata", {}).get("summary", {})

            data_list = response.get("data", [])
            success_ids = [x.get("order_id") for x in data_list if x.get("order_id")]
            errors = response.get("errors", [])

            # Atomic Check: All legs must be accepted
            if errors or len(success_ids) != len(trade.legs):
                logger.critical(f"❌ Batch Failure! Success: {len(success_ids)}/{len(trade.legs)}")
                # Immediate Rollback: Cancel any partial success
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
        """
        Polls for order completion using Exponential Backoff to prevent API Rate Limiting.
        """
        if settings.SAFETY_MODE != "live": return True

        # Exponential Backoff Schedule (Total ~30s)
        # Fast checks initially, then slower checks
        delays = [0.5, 0.5, 1.0, 1.0, 2.0, 2.0, 3.0, 5.0, 5.0, 5.0, 5.0]
        
        start_time = asyncio.get_event_loop().time()
        
        for delay in delays:
            # Check total timeout
            if (asyncio.get_event_loop().time() - start_time) > timeout:
                break
                
            all_filled = True
            
            # Check status of every leg in the batch
            for oid in trade.gtt_order_ids:
                try:
                    details = await self.api.get_order_details(oid)
                    
                    # Handle API response structure variations
                    if isinstance(details.get("data"), list):
                        # Some endpoints return list of history
                        order_data = details["data"][0] if details["data"] else {}
                    else:
                        order_data = details.get("data", {})
                        
                    status = str(order_data.get("status", "")).lower()
                    
                    if status == "complete":
                        continue
                    elif status in ["cancelled", "rejected", "error", "failure"]:
                        logger.error(f"❌ Leg {oid} Failed: {status.upper()}")
                        return False # Fail immediately on hard rejection
                    else:
                        # Status is open, pending, trigger_pending, etc.
                        all_filled = False
                        break # Stop checking other legs, wait for next cycle
                except Exception as e:
                    logger.warning(f"Error checking order {oid}: {e}")
                    all_filled = False
            
            if all_filled:
                logger.info(f"✅ Trade {trade.id} Fully Filled ({asyncio.get_event_loop().time() - start_time:.1f}s)")
                return True
            
            # Wait before next check (Backoff)
            await asyncio.sleep(delay)
            
        logger.warning(f"⚠️ Fill Verification Timeout {trade.id} after {timeout}s")
        return False
