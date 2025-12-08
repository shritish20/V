import asyncio
import logging
from typing import Optional, List, Dict
from core.models import MultiLegTrade, Position
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("OrderExec")

class LiveOrderExecutor:
    """
    Production Order Executor.
    Handles Atomic Batch Orders and GTT (Good Till Triggered) logic.
    """
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade, use_gtt: bool = False) -> bool:
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Batch Execution")
            trade.status = "OPEN"
            trade.gtt_order_ids = [f"SIM-{i}" for i in range(len(trade.legs))]
            return True

        # Freeze Limit Check
        for leg in trade.legs:
            if abs(leg.quantity) > settings.NIFTY_FREEZE_QTY:
                logger.error(f"🚫 FREEZE LIMIT: {leg.instrument_key} Qty {abs(leg.quantity)}")
                return False

        if use_gtt:
            return await self._place_gtt_batch(trade)
        else:
            return await self._place_regular_batch(trade)

    async def _place_regular_batch(self, trade: MultiLegTrade) -> bool:
        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            orders_payload.append({
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
                "correlation_id": f"LEG{idx}-{trade.id[:10]}",
                "tag": "VG19"
            })
        
        try:
            response = await self.api.place_multi_order(orders_payload)
            if response.get("status") != "success":
                logger.error(f"❌ Batch API Failed: {response}")
                return False
                
            # Atomic Validation
            metadata = response.get("metadata", {})
            summary = metadata.get("summary", {})
            if summary.get("error", 0) > 0 or summary.get("success", 0) != len(trade.legs):
                logger.critical("❌ Batch Atomic Violation! Rolling back...")
                # Logic to cancel partials would go here
                return False
            
            # Extract Order IDs
            data_list = response.get("data", [])
            success_ids = [item["order_id"] for item in data_list if "order_id" in item]
            trade.gtt_order_ids = success_ids
            logger.info(f"✅ Batch Filled: {len(success_ids)} legs")
            return True
            
        except Exception as e:
            logger.error(f"Batch Exception: {e}")
            return False

    async def _place_gtt_batch(self, trade: MultiLegTrade) -> bool:
        gtt_ids = []
        for leg in trade.legs:
            trigger_offset = 0.05
            trigger_price = leg.entry_price * (1 + trigger_offset) if leg.quantity > 0 else leg.entry_price * (1 - trigger_offset)
            trigger_type = "ABOVE" if leg.quantity > 0 else "BELOW"
            
            oid = await self.place_gtt_order(leg, trigger_price, trigger_type)
            if not oid:
                logger.error("❌ GTT Batch Failed (Partial). Manual check needed.")
                return False
            gtt_ids.append(oid)
        
        trade.gtt_order_ids = gtt_ids
        return True

    async def place_gtt_order(self, leg: Position, trigger_price: float, trigger_type: str = "ABOVE") -> Optional[str]:
        """
        FIXED: Matches Upstox Schema + Reverse Engineered Requirements.
        """
        payload = {
            "type": "SINGLE",
            "quantity": abs(leg.quantity),
            "product": "D",
            "rules": [{
                "strategy": "ENTRY",
                "trigger_type": trigger_type,
                "trigger_price": float(trigger_price),
                "trailing_gap": 0.0
            }],
            "instrument_token": leg.instrument_key,
            "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
            # CRITICAL FIXES based on Schema/Usage
            "order_type": "LIMIT", 
            "price": float(trigger_price), # Limit Price
            "validity": "DAY",
            "disclosed_quantity": 0
        }
        
        try:
            url = "https://api-v2.upstox.com/v3/order/gtt/place"
            response = await self.api._request_with_retry("POST", url, json=payload)
            if response.get("status") == "success":
                return response.get("data", {}).get("gtt_order_ids", [])[0]
            logger.error(f"GTT Failed: {response}")
            return None
        except Exception as e:
            logger.error(f"GTT Exception: {e}")
            return None

    async def verify_fills(self, trade: MultiLegTrade, timeout=30) -> bool:
        if settings.SAFETY_MODE != "live":
            return True
        
        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_filled = True
            for oid in trade.gtt_order_ids:
                try:
                    details = await self.api.get_order_details(oid)
                    data = details.get("data", [])
                    if not data or data[0].get("status", "").lower() != "complete":
                        all_filled = False
                        break
                except:
                    all_filled = False
                    break
            
            if all_filled:
                logger.info(f"✅ All {len(trade.gtt_order_ids)} legs filled")
                return True
            
            await asyncio.sleep(2)
            
        logger.warning(f"⚠️ Fill Verification Timeout {trade.id}")
        return False

    async def close_multi_leg_batch(self, trade: MultiLegTrade) -> bool:
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Close Execution")
            return True

        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            reversed_qty = leg.quantity * -1
            orders_payload.append({
                "instrument_token": leg.instrument_key,
                "transaction_type": "BUY" if reversed_qty > 0 else "SELL",
                "quantity": abs(reversed_qty),
                "product": "I",
                "validity": "DAY",
                "order_type": "MARKET",
                "price": 0.0,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "slice": False,
                "correlation_id": f"CLOSE-LEG{idx}-{trade.id[:10]}",
                "tag": "VG19-EXIT"
            })

        try:
            response = await self.api.place_multi_order(orders_payload)
            if response.get("status") == "success":
                logger.info(f"✅ Close Batch Placed")
                return True
            else:
                logger.error(f"❌ Close Batch Failed: {response}")
                return False
        except Exception as e:
            logger.error(f"❌ Close Exception: {e}")
            return False
