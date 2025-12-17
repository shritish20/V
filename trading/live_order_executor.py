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
    
    INSTITUTIONAL GRADE V2.1:
    - Hedge-First Execution: Always places BUY legs before SELL legs in the batch.
    - Intelligent Rollback: Uses Limit orders to avoid slippage on exits.
    - Aggressive Chunking: Respects API limits.
    - Schema Compliance: Strict 20-char limit on correlation_id.
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
            limit = settings.NIFTY_FREEZE_QTY
            if abs(leg.quantity) > limit:
                logger.error(f"🚫 FREEZE LIMIT: {leg.symbol} Qty {abs(leg.quantity)} > {limit}")
                return False

        if use_gtt:
            success = await self._place_gtt_batch(trade)
            if success:
                asyncio.create_task(self.monitor_gtt_oco(trade))
            return success
        else:
            return await self._place_regular_batch(trade)

    async def _place_regular_batch(self, trade: MultiLegTrade) -> bool:
        """
        Executes orders in batches. 
        CRITICAL FIX: STRICT CORRELATION ID CLIPPING (Max 20 chars).
        """
        orders_payload = []
        
        # 1. Build Payload Objects
        for idx, leg in enumerate(trade.legs):
            # SCHEMA SAFETY: correlation_id max length is 20.
            # Format: L{idx}-{trade_id_fragment}
            # "L0-" (3 chars) + 17 chars of ID
            t_id_frag = str(trade.id).replace("T-", "")[:15]
            corr_id = f"L{idx}-{t_id_frag}"

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
                "correlation_id": corr_id,
                "tag": f"VG19-{trade.id}"[:20] # Tag also often has limits, clipped safe
            })

        # 2. HEDGE FIRST SORTING (Institutional Requirement)
        # We sort the payload so that 'BUY' orders come before 'SELL' orders.
        # Lambda logic: 'BUY' < 'SELL' alphabetically? No, B < S is True.
        # So sorting by transaction_type ("BUY", "SELL") naturally puts BUY first.
        orders_payload.sort(key=lambda x: x["transaction_type"])

        # 3. Batch Execution with Limits
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
                    break 

                # Atomic Validation
                metadata = response.get("metadata", {})
                summary = metadata.get("summary", {})
                data_list = response.get("data", [])
                
                chunk_success_ids = [item["order_id"] for item in data_list if "order_id" in item]
                all_success_ids.extend(chunk_success_ids)

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
            logger.info(f"✅ Full Batch Filled: {len(all_success_ids)} legs (Sorted Hedge-First)")
            return True
        else:
            if all_success_ids:
                logger.critical(f"⚠️ Triggering Atomic Rollback for {len(all_success_ids)} orphaned legs...")
                await self._rollback_partial_orders(all_success_ids)
            return False

    async def _rollback_partial_orders(self, order_ids: List[str]):
        if not order_ids: return
        for oid in order_ids:
            try:
                details = await self.api.get_order_details(oid)
                data = details.get("data", [])
                if not data: continue
                
                order_data = data[0]
                status = str(order_data.get("status", "")).lower()

                if status == "complete":
                    await self._emergency_exit_filled_order(order_data)
                elif status in ["open", "pending", "trigger pending"]:
                    res = await self.api.cancel_order(oid)
                    if res: logger.info(f"✔ Rollback: Cancelled Pending Order {oid}")
                    else: logger.error(f"❌ Rollback: Failed to cancel {oid}")
            except Exception as e:
                logger.critical(f"❌ Rollback CRITICAL FAILURE for {oid}: {e}")

    async def _emergency_exit_filled_order(self, order_data: Dict):
        try:
            original_txn = order_data.get("transaction_type")
            exit_txn = "SELL" if original_txn == "BUY" else "BUY"
            
            avg_price = float(order_data.get("average_price", 0.0))
            if avg_price == 0: avg_price = float(order_data.get("price", 0.0))

            # Aggressive Limit Logic to prevent slippage
            if exit_txn == "BUY":
                limit_price = avg_price * 1.02
            else:
                limit_price = avg_price * 0.98
            
            limit_price = round(limit_price / 0.05) * 0.05

            logger.warning(f"⚠️ ROLLING BACK {order_data.get('instrument_token')} with AGGRESSIVE LIMIT @ {limit_price}")
            
            payload = {
                "quantity": int(order_data.get("quantity", 0)),
                "product": order_data.get("product", "I"),
                "validity": "DAY",
                "price": limit_price,
                "tag": "VG19-ROLLBACK",
                "instrument_token": order_data.get("instrument_token"),
                "order_type": "LIMIT",
                "transaction_type": exit_txn,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False
            }
            
            success, exit_id = await self.api.place_order_raw(payload)
            if not success:
                res = await self.api.place_multi_order([payload])
                if res.get("status") != "success":
                    logger.critical(f"❌ Rollback: EMERGENCY EXIT FAILED! MANUAL ACTION REQUIRED.")
            else:
                logger.warning(f"✔ Rollback: EMERGENCY EXIT Placed {exit_id}")

        except Exception as e:
            logger.critical(f"❌ Emergency Exit Failed: {e}")

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
            if not oid: return False
            gtt_ids.append(oid)
        
        trade.gtt_order_ids = gtt_ids
        return True

    async def place_gtt_order(self, leg: Position, trigger_price: float, trigger_type: str = "ABOVE") -> Optional[str]:
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
            "transaction_type": "BUY" if leg.quantity > 0 else "SELL"
        }
        try:
            url = "https://api-v2.upstox.com/v3/order/gtt/place"
            response = await self.api._request_with_retry("POST", url, json=payload)
            if response.get("status") == "success":
                return response.get("data", {}).get("gtt_order_ids", [])[0]
            return None
        except Exception as e:
            logger.error(f"GTT Exception: {e}")
            return None

    async def monitor_gtt_oco(self, trade: MultiLegTrade):
        logger.info(f"👀 GTT Janitor started for {trade.id}")
        active_ids = set(trade.gtt_order_ids)
        
        while active_ids and trade.status == "OPEN":
            triggered_any = False
            for gtt_id in list(active_ids):
                try:
                    details = await self.api.get_order_details(gtt_id)
                    data = details.get("data", [])
                    if not data: continue
                    
                    status = str(data[0].get("status", "")).upper()
                    if status in ["TRIGGERED", "FILLED", "COMPLETE"]:
                        logger.info(f"⚡ GTT {gtt_id} Triggered! Cancelling siblings...")
                        active_ids.remove(gtt_id)
                        triggered_any = True
                        break 
                except Exception:
                    pass
            
            if triggered_any:
                cancel_tasks = [self.api.cancel_order(oid) for oid in active_ids]
                if cancel_tasks: await asyncio.gather(*cancel_tasks)
                return 
            
            await asyncio.sleep(5)

    async def verify_fills(self, trade: MultiLegTrade, timeout=30) -> bool:
        if settings.SAFETY_MODE != "live": return True
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_filled = True
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
                        avg_price = float(order_data.get("average_price", 0.0))
                        if avg_price > 0 and hasattr(trade.legs[idx], 'entry_price'):
                            trade.legs[idx].entry_price = avg_price
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
        if settings.SAFETY_MODE != "live": return True
        
        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            reversed_qty = leg.quantity * -1
            
            # Use same correlation ID logic for tracking
            t_id_frag = str(trade.id).replace("T-", "")[:15]
            corr_id = f"C{idx}-{t_id_frag}"

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
                "correlation_id": corr_id,
                "tag": f"VG19-EXIT-{trade.id}"[:20]
            })
        
        # CLOSE SAFETY: Sort BUYs first here too (Cover Shorts first)
        orders_payload.sort(key=lambda x: x["transaction_type"])

        BATCH_LIMIT = 20
        chunks = [orders_payload[i:i + BATCH_LIMIT] for i in range(0, len(orders_payload), BATCH_LIMIT)]
        
        overall_success = True
        for i, chunk in enumerate(chunks):
            try:
                response = await self.api.place_multi_order(chunk)
                if response.get("status") != "success":
                    logger.error(f"❌ Close Batch Chunk {i+1} Failed: {response}")
                    overall_success = False
            except Exception as e:
                logger.error(f"❌ Close Exception in Chunk {i+1}: {e}")
                overall_success = False
                
        return overall_success
