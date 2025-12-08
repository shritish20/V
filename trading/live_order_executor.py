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
    
    CRITICAL FIXES v2.0:
    1. Implements intelligent rollback (Exit filled legs vs Cancel pending legs).
    2. Handles API batch limits by chunking requests.
    """
    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api

    async def place_multi_leg_batch(self, trade: MultiLegTrade, use_gtt: bool = False) -> bool:
        if settings.SAFETY_MODE != "live":
            logger.info(f"[{settings.SAFETY_MODE}] Sim Batch Execution")
            trade.status = "OPEN"
            trade.gtt_order_ids = [f"SIM-{i}" for i in range(len(trade.legs))]
            return True

        # STRICT NIFTY 50 FREEZE CHECK
        for leg in trade.legs:
            # Enforce NIFTY 50 limits only (1800 qty)
            limit = settings.NIFTY_FREEZE_QTY
                
            if abs(leg.quantity) > limit:
                logger.error(f"🚫 FREEZE LIMIT: {leg.symbol} Qty {abs(leg.quantity)} > {limit}")
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
        """
        Executes orders in batches. Handles splitting for API limits and atomic rollbacks.
        """
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

        # CRITICAL FIX: Upstox API limit is usually 20 orders per batch.
        # If we have extensive slicing, we must chunk the request.
        BATCH_LIMIT = 20
        chunks = [orders_payload[i:i + BATCH_LIMIT] for i in range(0, len(orders_payload), BATCH_LIMIT)]
        
        all_success_ids = []
        overall_success = True

        for i, chunk in enumerate(chunks):
            try:
                # Endpoint: /v2/order/multi/place
                response = await self.api.place_multi_order(chunk)
                
                if response.get("status") != "success":
                    logger.error(f"❌ Batch Chunk {i+1} Failed: {response}")
                    overall_success = False
                    break # Stop processing further chunks

                # Atomic Validation
                metadata = response.get("metadata", {})
                summary = metadata.get("summary", {})
                data_list = response.get("data", [])
                
                # Extract Successful Order IDs from this chunk
                chunk_success_ids = [item["order_id"] for item in data_list if "order_id" in item]
                all_success_ids.extend(chunk_success_ids)

                # Partial Failure Check within chunk
                if summary.get("error", 0) > 0 or summary.get("success", 0) != len(chunk):
                    logger.critical(f"❌ Batch Atomic Violation in Chunk {i+1}! Success: {len(chunk_success_ids)}/{len(chunk)}")
                    overall_success = False
                    break

            except Exception as e:
                logger.error(f"Batch Chunk Exception: {e}")
                overall_success = False
                break

        if overall_success:
            trade.gtt_order_ids = all_success_ids
            logger.info(f"✅ Full Batch Filled: {len(all_success_ids)} legs")
            return True
        else:
            # TRIGGER INTELLIGENT ROLLBACK
            if all_success_ids:
                logger.critical(f"⚠️ Triggering Atomic Rollback for {len(all_success_ids)} orphaned legs...")
                await self._rollback_partial_orders(all_success_ids)
            return False

    async def _rollback_partial_orders(self, order_ids: List[str]):
        """
        Emergency Rollback: 
        1. Checks status of each order.
        2. If FILLED -> Places Market Exit (Reverse trade).
        3. If PENDING -> Cancels order.
        """
        if not order_ids: return

        for oid in order_ids:
            try:
                # Fetch latest status
                details = await self.api.get_order_details(oid)
                data = details.get("data", [])
                
                if not data:
                    logger.error(f"❌ Rollback Error: Could not fetch details for {oid}")
                    continue

                order_data = data[0]
                status = str(order_data.get("status", "")).lower()
                
                # CASE 1: Order is already complete - WE MUST EXIT IT
                if status == "complete":
                    await self._emergency_exit_filled_order(order_data)
                
                # CASE 2: Order is pending/open - WE CAN CANCEL IT
                elif status in ["open", "pending", "trigger pending"]:
                    res = await self.api.cancel_order(oid)
                    if res:
                        logger.info(f"✔ Rollback: Cancelled Pending Order {oid}")
                    else:
                        logger.error(f"❌ Rollback: Failed to cancel {oid}")
                
                # CASE 3: Already rejected/cancelled - Do nothing
                else:
                    logger.debug(f"Rollback: Order {oid} is already {status}")

            except Exception as e:
                logger.critical(f"❌ Rollback CRITICAL FAILURE for {oid}: {e}")

    async def _emergency_exit_filled_order(self, order_data: Dict):
        """Helper to place an opposing market order for a filled leg"""
        try:
            original_txn = order_data.get("transaction_type")
            qty = int(order_data.get("quantity", 0))
            instrument_token = order_data.get("instrument_token")
            
            # Reverse logic
            exit_txn = "SELL" if original_txn == "BUY" else "BUY"
            
            payload = {
                "quantity": qty,
                "product": order_data.get("product", "I"),
                "validity": "DAY",
                "price": 0.0,
                "tag": "VG19-ATOMIC-ROLLBACK",
                "instrument_token": instrument_token,
                "order_type": "MARKET",
                "transaction_type": exit_txn,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False
            }
            
            # Place single order for emergency exit
            success, exit_id = await self.api.place_order_raw(payload) # Assuming API has raw placement or use place_order logic
            
            # Fallback if place_order_raw isn't exposed, use place_multi_order for consistency
            if not success:
                 res = await self.api.place_multi_order([payload])
                 if res.get("status") == "success":
                     logger.warning(f"✔ Rollback: EMERGENCY EXIT Placed for {instrument_token} (Qty: {qty})")
                 else:
                     logger.critical(f"❌ Rollback: EMERGENCY EXIT FAILED for {instrument_token}! MANUAL ACTION REQUIRED.")
            else:
                 logger.warning(f"✔ Rollback: EMERGENCY EXIT Placed {exit_id}")

        except Exception as e:
            logger.critical(f"❌ Emergency Exit Logic Error: {e}")

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
        Matches GttPlaceOrderRequest Schema.
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

        # CRITICAL FIX: Also handle batch limits for Closing trades
        BATCH_LIMIT = 20
        chunks = [orders_payload[i:i + BATCH_LIMIT] for i in range(0, len(orders_payload), BATCH_LIMIT)]
        
        overall_success = True
        
        for i, chunk in enumerate(chunks):
            try:
                response = await self.api.place_multi_order(chunk)
                if response.get("status") == "success":
                    logger.info(f"✅ Close Batch Chunk {i+1} Placed")
                else:
                    logger.error(f"❌ Close Batch Chunk {i+1} Failed: {response}")
                    overall_success = False
            except Exception as e:
                logger.error(f"❌ Close Exception in Chunk {i+1}: {e}")
                overall_success = False

        return overall_success
