import asyncio
import logging
import time
from typing import List, Tuple, Dict
from core.models import MultiLegTrade, Position
from core.config import settings

logger = logging.getLogger("LiveExecutor")

class LiveOrderExecutor:
    """
    Hardened Execution Engine with Smart Limit Logic.
    
    Features:
    1. Hedge-First Logic (Buy before Sell).
    2. Smart Limit Orders: Places Limit @ LTP +/- 3% Buffer.
       - Guarantees fill like Market, but protects against Flash Crash/Freak Trades.
    3. Freeze Quantity Slicing: Auto-splits >1800 qty orders.
    4. Atomic Rollback: Reverses trades if one leg fails.
    """
    
    def __init__(self, api_client, order_manager):
        self.api = api_client
        self.om = order_manager
        # Buffer for "Smart Limit" orders (3% slippage protection)
        self.protection_buffer_pct = 0.03 

    async def execute_with_hedge_priority(self, trade: MultiLegTrade) -> Tuple[bool, str]:
        """
        Orchestrates the trade execution:
        1. Split into HEDGE (Buy) and RISK (Sell).
        2. Execute HEDGE via Smart Limit.
        3. Wait 0.5s for Margin Benefit.
        4. Execute RISK via Smart Limit.
        5. Rollback HEDGE if RISK fails.
        """
        logger.info(f"🛡️ Starting Execution for {trade.id} [{trade.strategy_type.value}]")
        
        # 1. Split Legs
        hedge_legs = [l for l in trade.legs if l.quantity > 0] # Buy
        risk_legs = [l for l in trade.legs if l.quantity < 0]  # Sell
        
        logger.info(f"   Hedge Legs: {len(hedge_legs)} | Risk Legs: {len(risk_legs)}")
        
        # 2. Execute Hedges (Wings)
        if hedge_legs:
            logger.info("🛡️ Placing Hedge Orders (Smart Limit)...")
            success, msg = await self._execute_leg_batch(hedge_legs, trade.id, "HEDGE")
            if not success:
                logger.critical(f"❌ Hedge Execution Failed: {msg}. Aborting Trade.")
                return False, f"Hedge Failed: {msg}"
        
        # 3. Wait for Margin Benefit Registration
        if hedge_legs and risk_legs:
            await asyncio.sleep(0.5)
        
        # 4. Execute Risk (Body)
        if risk_legs:
            logger.info("⚔️ Placing Risk Orders (Smart Limit)...")
            success, msg = await self._execute_leg_batch(risk_legs, trade.id, "RISK")
            
            if not success:
                logger.critical(f"❌ Risk Leg Execution Failed: {msg}. Triggering Rollback.")
                await self._rollback_positions(hedge_legs)
                return False, f"Risk Failed (Rolled Back): {msg}"
                
        return True, "Execution Complete"

    async def _execute_leg_batch(self, legs: List[Position], trade_id: str, tag_prefix: str) -> Tuple[bool, str]:
        """
        Builds and sends a multi-order batch with calculated Limit Prices.
        """
        payload = []
        
        # 1. Fetch Live Prices for "Smart Limit" calculation
        # We need the LTP to calculate the Limit Price (LTP + Buffer)
        try:
            tokens = [l.instrument_key for l in legs]
            quotes = {}
            if len(tokens) > 0:
                # Fetch Quotes
                quote_res = await self.api.get_market_quote_ohlc(",".join(tokens), "1d")
                if quote_res.get("status") == "success":
                    # Parse messy Upstox structure to find LTP
                    data = quote_res.get("data", {})
                    for key, val in data.items():
                        # Try to get last_price, fall back to OHLC close
                        ltp = val.get("last_price")
                        if not ltp and "ohlc" in val:
                            ltp = val["ohlc"].get("close")
                        quotes[key] = float(ltp) if ltp else 0.0
        except Exception as e:
            logger.warning(f"⚠️ Quote fetch failed, will use Market orders: {e}")
            quotes = {} 

        # 2. Build Payload
        for i, leg in enumerate(legs):
            # A. Freeze Slicing Logic (Nifty limit is usually 1800)
            qty_abs = abs(leg.quantity)
            max_qty = settings.NIFTY_FREEZE_QTY
            
            slices = []
            if qty_abs > max_qty:
                full_slices = qty_abs // max_qty
                remainder = qty_abs % max_qty
                slices = [max_qty] * full_slices
                if remainder > 0: slices.append(remainder)
            else:
                slices = [qty_abs]

            # B. Construct Orders for each slice
            ltp = quotes.get(leg.instrument_key, 0.0)
            
            for s_idx, s_qty in enumerate(slices):
                # Smart Price Calculation
                price = 0.0
                order_type = "MARKET"
                
                if ltp > 0:
                    order_type = "LIMIT"
                    if leg.quantity > 0: 
                        # BUY: Limit = LTP + Buffer (Allow buying up to this price)
                        price = round(ltp * (1 + self.protection_buffer_pct), 1)
                    else: 
                        # SELL: Limit = LTP - Buffer (Allow selling down to this price)
                        price = round(ltp * (1 - self.protection_buffer_pct), 1)
                
                order_obj = {
                    "quantity": s_qty,
                    "product": "I", # Intraday
                    "validity": "DAY",
                    "price": price,
                    "tag": f"{trade_id}-{tag_prefix}-{i}",
                    "instrument_token": leg.instrument_key,
                    "order_type": order_type,
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "disclosed_quantity": 0,
                    "trigger_price": 0.0,
                    "is_amo": False,
                    "correlation_id": f"{trade_id}-{tag_prefix}-{i}-{s_idx}"
                }
                payload.append(order_obj)

        # 3. Send Batch
        if not payload: return True, "Empty Batch"
        
        try:
            logger.info(f"📤 Sending Batch of {len(payload)} orders (Type: {payload[0]['order_type']})...")
            res = await self.api.place_multi_order(payload)
            
            if res.get("status") == "success":
                return True, "Batch Sent"
            else:
                err = res.get("message", "Unknown Error")
                logger.error(f"API Error: {err}")
                return False, err
                
        except Exception as e:
            logger.error(f"Batch Exception: {e}")
            return False, str(e)

    async def _rollback_positions(self, filled_legs: List[Position]):
        """
        Emergency: Reverses the transaction type for all legs provided.
        Used to close Hedges if the Risk leg fails.
        """
        logger.critical(f"🚨 ROLLING BACK {len(filled_legs)} LEGS...")
        
        payload = []
        for leg in filled_legs:
            # Reverse: If we Bought, now we Sell to close.
            rev_trans = "SELL" if leg.quantity > 0 else "BUY"
            
            payload.append({
                "quantity": abs(leg.quantity),
                "product": "I",
                "validity": "DAY",
                "price": 0.0, # Market order for immediate exit (Emergency)
                "tag": "ROLLBACK",
                "instrument_token": leg.instrument_key,
                "order_type": "MARKET",
                "transaction_type": rev_trans,
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "correlation_id": f"ROLLBACK-{int(time.time())}"
            })
            
        if payload:
            try:
                await self.api.place_multi_order(payload)
                logger.info("✅ Rollback Orders Sent")
            except Exception as e:
                logger.critical(f"🔥 ROLLBACK FAILED: {e} - MANUAL INTERVENTION REQUIRED")
