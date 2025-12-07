import asyncio
import logging
from typing import Optional, List, Dict
from core.models import MultiLegTrade, OrderStatus
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    """
    Handles Atomic Multi-Leg Order Placement using Upstox v2 API.
    Ensures all legs are placed together or rejected together.
    """
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        """
        Executes all legs in a single API call.
        Returns True only if the batch was accepted by Upstox.
        """
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Simulating Batch Execution for {trade.id}")
            trade.basket_order_id = f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"
            trade.status = "OPEN"
            return True

        orders_payload = []
        
        # 1. Build Payload
        for idx, leg in enumerate(trade.legs):
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
                "slice": False,  # Critical for atomic behavior
                "correlation_id": f"{trade.id}-LEG{idx}", # Unique ID per leg
                "tag": "VG19"
            }
            orders_payload.append(order)

        # 2. Submit Batch
        try:
            url = f"{settings.API_BASE_V2}/order/multi/place"
            response = await self.api._request_with_retry("POST", url, json=orders_payload)
            
            # 3. Validation Logic (The Counter to API Misuse)
            if response.get("status") == "success":
                data = response.get("data", [])
                
                # Check for payload errors (Upstox specific check)
                # If any order has an error in the summary, we treat it as risky
                # Note: Upstox V2 usually rejects the whole JSON if format is wrong,
                # but we check the response list for individual failures.
                
                order_ids = [item.get("order_id") for item in data if item.get("order_id")]
                
                if len(order_ids) != len(trade.legs):
                    logger.critical(f"❌ Batch Partial Failure! Sent {len(trade.legs)}, got {len(order_ids)} IDs.")
                    # Emergency handling: Cancel what we got
                    for oid in order_ids:
                        await self.api.cancel_order(oid)
                    return False
                
                trade.gtt_order_ids = order_ids
                trade.basket_order_id = order_ids[0] # Use first ID as ref
                logger.info(f"✅ Batch Accepted. Ref ID: {trade.basket_order_id}")
                return True
            
            else:
                logger.error(f"❌ Batch Rejected: {response}")
                return False

        except Exception as e:
            logger.error(f"❌ Batch Exception: {e}")
            return False

    async def verify_fills(self, trade: MultiLegTrade, timeout=30) -> bool:
        """
        Polls order status to ensure all legs are FILLED.
        """
        if settings.SAFETY_MODE != "live":
            return True

        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_filled = True
            
            for oid in trade.gtt_order_ids:
                details = await self.api.get_order_details(oid)
                status = details.get("data", {}).get("status")
                
                if status != "complete":
                    all_filled = False
                    if status in ["cancelled", "rejected"]:
                        logger.error(f"❌ Order {oid} failed with status: {status}")
                        return False # Fail immediately if any leg dies
            
            if all_filled:
                return True
            
            await asyncio.sleep(1)
            
        logger.warning(f"⚠️ Fill Verification Timed Out for trade {trade.id}")
        return False
