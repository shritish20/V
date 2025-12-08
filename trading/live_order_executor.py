import asyncio
import logging
from typing import Optional, List, Dict
from core.models import MultiLegTrade, Position
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    """
    PRODUCTION FIXED:
    - Schema-compliant multi-order placement
    - GTT order support for overnight hedging
    - Proper freeze quantity validation
    - Atomic rollback on partial fills
    """
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade, use_gtt: bool = False) -> bool:
        """
        PRODUCTION FIX: Added GTT support flag for overnight strategies.
        """
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Batch Execution")
            trade.basket_order_id = f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"
            trade.status = "OPEN"
            trade.gtt_order_ids = [f"SIM-ORD-{i}" for i in range(len(trade.legs))]
            return True

        # PRODUCTION FIX: Check if any leg violates freeze limits
        for leg in trade.legs:
            if abs(leg.quantity) > settings.NIFTY_FREEZE_QTY:
                logger.error(
                    f"🚫 FREEZE LIMIT VIOLATION: {leg.instrument_key} "
                    f"has {abs(leg.quantity)} qty > {settings.NIFTY_FREEZE_QTY}"
                )
                return False

        # Route to GTT or Regular order placement
        if use_gtt:
            return await self._place_gtt_batch(trade)
        else:
            return await self._place_regular_batch(trade)

    async def _place_regular_batch(self, trade: MultiLegTrade) -> bool:
        """Standard intraday multi-order placement"""
        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            order = {
                "instrument_token": leg.instrument_key,
                "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                "quantity": abs(leg.quantity),
                "product": "I",  # Intraday
                "validity": "DAY",
                "order_type": "MARKET" if leg.entry_price <= 0 else "LIMIT",
                "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "slice": False,
                "correlation_id": f"LEG{idx}-{trade.id[:10]}",
                "tag": "VG19"
            }
            orders_payload.append(order)

        try:
            response = await self.api.place_multi_order(orders_payload)
            
            if response.get("status") != "success":
                logger.error(f"❌ Batch API Failed: {response}")
                return False

            # Parse response
            data_list = response.get("data", [])
            errors = response.get("errors", [])

            success_ids = []
            for item in data_list:
                if "order_id" in item:
                    success_ids.append(item["order_id"])

            # PRODUCTION FIX: Atomic check - all or nothing
            if errors or len(success_ids) != len(trade.legs):
                logger.critical(
                    f"❌ Batch Partial Fill! Success: {len(success_ids)}/{len(trade.legs)}"
                )
                # Rollback all successful orders
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

    async def _place_gtt_batch(self, trade: MultiLegTrade) -> bool:
        """
        PRODUCTION NEW: GTT (Good Till Triggered) order placement.
        Used for overnight hedging or post-market triggers.
        """
        gtt_order_ids = []
        
        for idx, leg in enumerate(trade.legs):
            # Calculate trigger price (example: 5% above/below current price)
            trigger_offset = 0.05
            if leg.quantity > 0:  # BUY leg
                trigger_price = leg.entry_price * (1 + trigger_offset)
                trigger_type = "ABOVE"
            else:  # SELL leg
                trigger_price = leg.entry_price * (1 - trigger_offset)
                trigger_type = "BELOW"

            gtt_order_id = await self.place_gtt_order(leg, trigger_price, trigger_type)
            
            if not gtt_order_id:
                logger.error(f"❌ GTT Order Failed for Leg {idx}")
                # Rollback previous GTT orders
                for prev_id in gtt_order_ids:
                    await self.cancel_gtt_order(prev_id)
                return False
            
            gtt_order_ids.append(gtt_order_id)
        
        trade.gtt_order_ids = gtt_order_ids
        trade.basket_order_id = gtt_order_ids[0]
        logger.info(f"✅ GTT Batch Placed: {len(gtt_order_ids)} orders")
        return True

    async def place_gtt_order(self, leg: Position, trigger_price: float, 
                             trigger_type: str = "ABOVE") -> Optional[str]:
        """
        PRODUCTION NEW: Place single GTT order.
        Schema: /v3/order/gtt/place
        """
        if settings.SAFETY_MODE != "live":
            return f"SIM-GTT-{int(asyncio.get_event_loop().time())}"
        
        payload = {
            "type": "SINGLE",
            "quantity": abs(leg.quantity),
            "product": "D",  # Delivery for GTT (can hold overnight)
            "rules": [{
                "strategy": "ENTRY",
                "trigger_type": trigger_type,
                "trigger_price": float(trigger_price),
                "trailing_gap": 0.0
            }],
            "instrument_token": leg.instrument_key,
            "transaction_type": "BUY" if leg.quantity > 0 else "SELL"
        }
        
        try:
            url = "https://api-v2.upstox.com/v3/order/gtt/place"
            response = await self.api._request_with_retry("POST", url, json=payload)
            
            if response.get("status") == "success":
                gtt_ids = response.get("data", {}).get("gtt_order_ids", [])
                if gtt_ids:
                    return gtt_ids[0]
            
            logger.error(f"GTT Placement Failed: {response}")
            return None
            
        except Exception as e:
            logger.error(f"GTT Exception: {e}")
            return None

    async def cancel_gtt_order(self, gtt_order_id: str) -> bool:
        """
        Cancel GTT order.
        Schema: /v3/order/gtt/cancel
        """
        if settings.SAFETY_MODE != "live":
            return True
        
        try:
            url = "https://api-v2.upstox.com/v3/order/gtt/cancel"
            payload = {"gtt_order_id": gtt_order_id}
            
            response = await self.api._request_with_retry("DELETE", url, json=payload)
            return response.get("status") == "success"
            
        except Exception as e:
            logger.error(f"GTT Cancel Failed: {e}")
            return False

    async def verify_fills(self, trade: MultiLegTrade, timeout=30) -> bool:
        """
        PRODUCTION FIX: Verify all legs are filled within timeout.
        Uses exponential backoff to avoid hammering API.
        """
        if settings.SAFETY_MODE != "live": 
            return True

        delays = [0.5, 1.0, 2.0, 3.0, 5.0, 5.0, 5.0, 5.0]
        start_time = asyncio.get_event_loop().time()
        
        for delay in delays:
            if (asyncio.get_event_loop().time() - start_time) > timeout:
                break
                
            all_filled = True
            for oid in trade.gtt_order_ids:
                try:
                    details = await self.api.get_order_details(oid)
                    data = details.get("data", [])
                    
                    if not data:
                        all_filled = False
                        break

                    # Latest status is first item in history array
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
                logger.info(f"✅ All {len(trade.gtt_order_ids)} legs filled")
                return True
            
            await asyncio.sleep(delay)
            
        logger.warning(f"⚠️ Fill Verification Timeout {trade.id}")
        return False

    async def close_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        """
        PRODUCTION NEW: Close existing trade by reversing all legs.
        """
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Close Execution")
            return True

        # Create reverse orders
        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            # Reverse quantity (Buy -> Sell, Sell -> Buy)
            reversed_qty = leg.quantity * -1
            
            order = {
                "instrument_token": leg.instrument_key,
                "transaction_type": "BUY" if reversed_qty > 0 else "SELL",
                "quantity": abs(reversed_qty),
                "product": "I",
                "validity": "DAY",
                "order_type": "MARKET",  # Market for quick exit
                "price": 0.0,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "slice": False,
                "correlation_id": f"CLOSE-LEG{idx}-{trade.id[:10]}",
                "tag": "VG19-EXIT"
            }
            orders_payload.append(order)

        try:
            response = await self.api.place_multi_order(orders_payload)
            
            if response.get("status") == "success":
                data_list = response.get("data", [])
                success_count = len([d for d in data_list if "order_id" in d])
                
                logger.info(f"✅ Close Orders Placed: {success_count}/{len(trade.legs)}")
                return success_count == len(trade.legs)
            else:
                logger.error(f"❌ Close Batch Failed: {response}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Close Exception: {e}")
            return False
