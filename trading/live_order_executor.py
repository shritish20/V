#!/usr/bin/env python3
"""
LiveOrderExecutor 20.1 – Production Hardened & Bug Fixed
- Idempotent orders (client-order-id via Blake2b)
- Correct Freeze Slicing (Shares vs Shares)
- Max-slippage guard with market-fallback
- Rollback state-machine – never leave naked risk
"""
from __future__ import annotations

import asyncio
import logging
import time
import hashlib
from typing import List, Tuple, Dict, Optional, Any

from core.models import MultiLegTrade, Position
from core.config import settings

logger = logging.getLogger("LiveExecutor")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MAX_SLIPPAGE_PCT = float(getattr(settings, "MAX_SLIPPAGE_PCT", 0.05))   # 5% Max allowed slippage
SMART_BUFFER_PCT = float(getattr(settings, "SMART_BUFFER_PCT", 0.03))   # 3% Limit Buffer
# Default Freeze Quantity (Shares) if not in settings
DEFAULT_FREEZE_QTY = 1800 
# ---------------------------------------------------------------------------


class RollbackFailure(RuntimeError):
    """Raised when rollback itself fails – engine must shut down."""


class LiveOrderExecutor:
    """Hardened multi-leg execution with hedge-first logic."""

    def __init__(self, api_client, order_manager) -> None:
        self.api = api_client
        self.om = order_manager

    # -------------------------------------------------------------------------
    # Public Entry Point
    # -------------------------------------------------------------------------
    async def execute_with_hedge_priority(
        self, trade: MultiLegTrade
    ) -> Tuple[bool, str]:
        """
        1. Hedge legs (buy)  -> smart-limit
        2. Wait 0.5s for margin benefit
        3. Risk legs (sell)  -> smart-limit
        4. If any risk leg fails -> rollback hedge
        5. If rollback fails   -> raise RollbackFailure (engine halt)
        """
        logger.info("🛡️ Execution started", extra={"trade_id": trade.id})

        # Identify Legs
        hedge_legs = [l for l in trade.legs if l.quantity > 0]
        risk_legs = [l for l in trade.legs if l.quantity < 0]

        # 1. Hedge side
        if hedge_legs:
            ok, msg = await self._execute_batch(hedge_legs, trade.id, "HEDGE")
            if not ok:
                return False, f"Hedge failed: {msg}"

        # 2. Margin benefit wait
        if hedge_legs and risk_legs:
            await asyncio.sleep(0.5)

        # 3. Risk side
        if risk_legs:
            ok, msg = await self._execute_batch(risk_legs, trade.id, "RISK")
            if not ok:
                logger.error("❌ Risk failed – rolling back hedge", extra={"trade_id": trade.id})
                await self._rollback(hedge_legs)
                return False, f"Risk failed (hedge rolled back): {msg}"

        logger.info("✅ Execution complete", extra={"trade_id": trade.id})
        return True, "All legs executed"

    # -------------------------------------------------------------------------
    # Batch Execution
    # -------------------------------------------------------------------------
    async def _execute_batch(
        self, legs: List[Position], trade_id: str, side: str
    ) -> Tuple[bool, str]:
        """Build idempotent order batch with slippage guard."""
        
        # Fetch Live Prices
        quotes = await self._fetch_quotes([l.instrument_key for l in legs])
        if not quotes:
            logger.warning("⚠️ No quotes – falling back to Market Orders", extra={"trade_id": trade_id})
            quotes = {}

        payload: List[Dict[str, Any]] = []
        
        for idx, leg in enumerate(legs):
            # FIX: Correct Slicing Logic (Shares vs Shares)
            slices = self._slice_quantity(abs(leg.quantity))
            ltp = quotes.get(leg.instrument_key, 0.0)

            for slice_idx, qty in enumerate(slices):
                order_type, price = self._derive_order_type_and_price(
                    ltp, leg.quantity > 0
                )
                
                # Generate Idempotent ID
                cid = self._client_order_id(trade_id, side, idx, slice_idx)
                
                payload.append(
                    {
                        "quantity": int(qty),
                        "product": "I",
                        "validity": "DAY",
                        "price": price,
                        "trigger_price": 0.0,
                        "instrument_token": leg.instrument_key,
                        "order_type": order_type,
                        "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                        "disclosed_quantity": 0,
                        "is_amo": False,
                        "tag": f"{trade_id}-{side}-{idx}",
                        "correlation_id": cid,  # <--- Idempotency Key
                    }
                )

        if not payload:
            return True, "Nothing to send"

        # Send Batch
        try:
            logger.info(f"📤 Sending batch of {len(payload)} orders", extra={"trade_id": trade_id})
            res = await self.api.place_multi_order(payload)
            
            if res.get("status") == "success":
                # Simplistic fill update - in prod, wait for WebSocket/Callback
                for leg in legs:
                    # Update leg with estimated entry price for PnL calculation
                    leg.entry_price = quotes.get(leg.instrument_key, 0.0)
                return True, "Batch ok"
            else:
                err = res.get("message", "Unknown Broker Error")
                logger.error("❌ Broker rejected batch", extra={"trade_id": trade_id, "error": err})
                return False, err
                
        except Exception as exc:
            logger.exception("🔥 Batch exception", extra={"trade_id": trade_id})
            return False, str(exc)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    async def _fetch_quotes(self, tokens: List[str]) -> Dict[str, float]:
        """Return token -> ltp mapping."""
        if not tokens: return {}
        try:
            res = await self.api.get_market_quote_ohlc(",".join(tokens), "1d")
            if res.get("status") != "success": return {}
            
            quotes = {}
            for tok, val in res.get("data", {}).items():
                # Prefer LTP, fallback to Close
                ltp = val.get("last_price") or (val.get("ohlc") or {}).get("close")
                if ltp: quotes[tok] = float(ltp)
            return quotes
        except Exception:
            logger.error("Quote fetch failed silently")
            return {}

    def _slice_quantity(self, qty: int) -> List[int]:
        """
        Correct Slicing Logic: 
        Splits total Quantity (Shares) by Freeze Quantity (Shares).
        """
        # Get freeze limit from settings, default to 1800 (Nifty)
        freeze_limit = getattr(settings, "NIFTY_FREEZE_QTY", DEFAULT_FREEZE_QTY)
        
        slices: List[int] = []
        if qty <= freeze_limit:
            return [qty]
            
        full_slices, remainder = divmod(qty, freeze_limit)
        slices.extend([freeze_limit] * full_slices)
        if remainder > 0:
            slices.append(remainder)
            
        return slices

    def _derive_order_type_and_price(
        self, ltp: float, is_buy: bool
    ) -> Tuple[str, Optional[float]]:
        """Smart-limit with fallback to market if limit is outside safety bounds."""
        if ltp <= 0:
            return "MARKET", 0.0

        limit_price = ltp * (1 + SMART_BUFFER_PCT) if is_buy else ltp * (1 - SMART_BUFFER_PCT)
        
        # Max Slippage Calculation (The Safety Guard)
        # If we are Buying, Price shouldn't be higher than LTP + 5%
        # If we are Selling, Price shouldn't be lower than LTP - 5%
        max_safety_price = ltp * (1 + MAX_SLIPPAGE_PCT) if is_buy else ltp * (1 - MAX_SLIPPAGE_PCT)

        # Check if our "Smart Limit" violates the safety guard
        is_unsafe = (is_buy and limit_price > max_safety_price) or \
                    (not is_buy and limit_price < max_safety_price)

        if not is_unsafe:
            return "LIMIT", round(limit_price, 1)
        else:
            logger.warning(
                "⚠️ Limit price exceeds max slippage - Forcing MARKET order",
                extra={"ltp": ltp, "limit": limit_price, "max_safe": max_safety_price}
            )
            return "MARKET", 0.0

    @staticmethod
    def _client_order_id(trade_id: str, side: str, leg_idx: int, slice_idx: int) -> str:
        """Idempotent key generation using Blake2b hashing."""
        raw = f"{trade_id}#{side}#{leg_idx}#{slice_idx}"
        # Generate hash and take first 20 chars to fit broker limits
        hash_digest = hashlib.blake2b(raw.encode(), digest_size=10).hexdigest().upper()
        return f"VG{hash_digest}"

    # -------------------------------------------------------------------------
    # Rollback Logic
    # -------------------------------------------------------------------------
    async def _rollback(self, legs: List[Position]) -> None:
        """Reverse all legs – if this fails we raise RollbackFailure."""
        if not legs: return

        logger.critical("🚨 ROLLBACK STARTED", extra={"leg_count": len(legs)})

        payload = []
        for leg in legs:
            payload.append(
                {
                    "quantity": abs(leg.quantity),
                    "product": "I",
                    "validity": "DAY",
                    "price": 0.0,  # Market order for immediate exit
                    "instrument_token": leg.instrument_key,
                    "order_type": "MARKET",
                    "transaction_type": "SELL" if leg.quantity > 0 else "BUY",
                    "disclosed_quantity": 0,
                    "is_amo": False,
                    "tag": "ROLLBACK",
                    "correlation_id": f"RB-{int(time.time() * 1000)}", # Unique ID for rollback
                }
            )

        try:
            res = await self.api.place_multi_order(payload)
            if res.get("status") != "success":
                raise RuntimeError(res.get("message", "Rollback rejected by Broker"))
            logger.info("✅ Rollback successful - Positions Closed")
        except Exception as exc:
            logger.critical("🔥 ROLLBACK FAILED – CRITICAL STATE", exc_info=True)
            # This exception MUST be caught by Engine to trigger Shutdown
            raise RollbackFailure("Unable to rollback – naked risk possible") from exc
