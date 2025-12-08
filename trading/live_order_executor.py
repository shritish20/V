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
    Handles Atomic Batch Orders and GTT (Good Till Triggered) logic with OCO monitoring.
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
            success = await self._place_gtt_batch(trade)
            if success:
                # Launch background janitor task to handle OCO logic
                asyncio.create_task(self.monitor_gtt_oco(trade))
            return success
        else:
            return await self._place_regular_batch(trade)

    async def _place_regular_batch(self, trade: MultiLegTrade) -> bool:
        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            # Schema: MultiOrderRequest
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
                "tag": f"VG19-{trade.id}" # Tagged with Trade ID for reconciliation
            })

        try:
            # Endpoint: /v2/order/multi/place
            response = await self.api.place_multi_order(orders_payload)
            
            if response.get("status") != "success":
                logger.error(f"❌ Batch API Failed: {response}")
                return False

            # Atomic Validation
            metadata = response.get("metadata", {})
            summary = metadata.get("summary", {})
            data_list = response.get("data", [])
            
            # Extract Successful Order IDs
            success_ids = [item["order_id"] for item in data_list if "order_id" in item]

            # Partial Failure Check
            if summary.get("error", 0) > 0 or summary.get("success", 0) != len(trade.legs):
                logger.critical(f"❌ Batch Atomic Violation! Success: {len(success_ids)}/{len(trade.legs)}")
                if success_ids:
                    await self._rollback_partial_orders(success_ids)
                return False

            trade.gtt_order_ids = success_ids
            logger.info(f"✅ Batch Filled: {len(success_ids)} legs")
            return True

        except Exception as e:
            logger.error(f"Batch Exception: {e}")
            return False

    async def _rollback_partial_orders(self, order_ids: List[str]):
        """Emergency Rollback: Cancels orders that were part of a failed batch."""
        if not order_ids: return
        logger.warning(f"⚠️ Rolling back {len(order_ids)} partial orders...")
        tasks = [self.api.cancel_order(oid) for oid in order_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for oid, res in zip(order_ids, results):
            if res is True:
                logger.info(f"✔ Rollback: Cancelled {oid}")
            else:
                logger.critical(f"❌ Rollback FAILED for {oid}. MANUAL INTERVENTION REQUIRED.")

    async def _place_gtt_batch(self, trade: MultiLegTrade) -> bool:
        gtt_ids = []
        for leg in trade.legs:
            trigger_offset = 0.05
            if leg.quantity > 0:
                trigger_price = leg.entry_price * (1 + trigger_offset)
                trigger_type = "ABOVE"
            else:
                trigger_price = leg.entry_price * (1 - trigger_offset)
                trigger_type = "BELOW"

            oid = await self.place_gtt_order(leg, trigger_price, trigger_type)
            if not oid:
                logger.error("❌ GTT Batch Failed (Partial). Manual check needed.")
                return False
            gtt_ids.append(oid)
        
        trade.gtt_order_ids = gtt_ids
        return True

    async def place_gtt_order(self, leg: Position, trigger_price: float, trigger_type: str = "ABOVE") -> Optional[str]:
        """
        FIXED: Matches GttPlaceOrderRequest Schema.
        Removes invalid fields (order_type, price, validity) that cause 400 errors.
        """
        payload = {
            "type": "SINGLE",
            "quantity": abs(leg.quantity),
            "product": "D",  # GTT is typically Delivery
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
                return response.get("data", {}).get("gtt_order_ids", [])[0]
            
            logger.error(f"GTT Failed: {response}")
            return None
        except Exception as e:
            logger.error(f"GTT Exception: {e}")
            return None

    async def monitor_gtt_oco(self, trade: MultiLegTrade):
        """
        The 'Janitor': Monitors GTT legs. If one fires, cancels the other(s).
        Crucial for preventing double exposure.
        """
        logger.info(f"👀 GTT Janitor started for {trade.id}")
        active_ids = set(trade.gtt_order_ids)
        
        # Monitor while the trade is technically "OPEN" and we have active GTTs
        while active_ids and trade.status == "OPEN":
            triggered_any = False
            for gtt_id in list(active_ids):
                try:
                    # Fetch details
                    details = await self.api.get_order_details(gtt_id)
                    data = details.get("data", [])
                    if not data: continue
                    
                    # Check status
                    status = str(data[0].get("status", "")).upper()
                    if status in ["TRIGGERED", "FILLED", "COMPLETE"]:
                        logger.info(f"⚡ GTT {gtt_id} Triggered! Cancelling siblings...")
                        active_ids.remove(gtt_id)
                        triggered_any = True
                        break # Break to cancel others
                except Exception as e:
                    logger.debug(f"GTT Monitor error: {e}")
            
            if triggered_any:
                # Cancel all remaining active GTTs for this trade
                cancel_tasks = [self.api.cancel_order(oid) for oid in active_ids]
                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks)
                logger.info(f"✔ GTT OCO Cleanup Complete for {trade.id}")
                return # Exit task

            await asyncio.sleep(5)

    async def verify_fills(self, trade: MultiLegTrade, timeout=30) -> bool:
        if settings.SAFETY_MODE != "live":
            return True

        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_filled = True
            
            # Check GTTs or Standard Orders (reusing gtt_order_ids field for IDs)
            for idx, oid in enumerate(trade.gtt_order_ids):
                try:
                    details = await self.api.get_order_details(oid)
                    data = details.get("data", [])
                    if not data:
                        all_filled = False
                        break
                    
                    order_data = data[0]
                    status = str(order_data.get("status", "")).lower()
                    
                    if status == "complete":
                        # CRITICAL FIX: Update entry price with ACTUAL execution price
                        avg_price = float(order_data.get("average_price", 0.0))
                        if avg_price > 0:
                            if hasattr(trade.legs[idx], 'entry_price'):
                                trade.legs[idx].entry_price = avg_price
                                logger.info(f"💲 Price Corrected: Leg {idx} filled at {avg_price}")
                    else:
                        all_filled = False
                        break
                except:
                    all_filled = False
                    break
            
            if all_filled:
                logger.info(f"✅ All legs filled & prices updated")
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
                "tag": f"VG19-EXIT-{trade.id}"
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
