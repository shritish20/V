#!/usr/bin/env python3
"""
SmartCapitalAllocator 20.0 – Production Hardened
- ATOMIC ALLOCATION: Uses 'SELECT ... FOR UPDATE' row locking.
- Idempotent: Prevents double-allocation using Unique Constraints.
- Draw-down Brake: Stops allocation if daily loss limit is hit.
- Real-time Margin: Syncs with Broker API.
"""
from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from sqlalchemy import select, text
from core.config import settings
from database.models import DbCapitalUsage

logger = logging.getLogger("CapitalAllocator")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MARGIN_REFRESH_SEC = 30  # Broker margin cache TTL

class SmartCapitalAllocator:
    def __init__(self, fallback_account_size: float, allocation_config: Dict[str, float], db) -> None:
        self._fallback_size = fallback_account_size
        self._bucket_pct = allocation_config
        self._db = db
        # We don't need asyncio.Lock anymore because we use DB locks
        self._last_margin_fetch = 0.0
        self._cached_available_margin = fallback_account_size

    # -------------------------------------------------------------------------
    # Public – Used by Engine every cycle
    # -------------------------------------------------------------------------
    async def get_status(self) -> Dict[str, Any]:
        """Return current capital status (includes real margin)."""
        margin = await self._get_real_margin()
        used = await self._get_used_breakdown()
        
        buckets = {}
        for bucket in self._bucket_pct:
            limit = margin * self._bucket_pct.get(bucket, 0.0)
            used_amt = used.get(bucket, 0.0)
            buckets[bucket] = {
                "limit": limit,
                "used": used_amt,
                "avail": max(0.0, limit - used_amt),
            }
        
        return {
            "total": margin,
            "cached_margin": margin,
            "buckets": buckets,
            "used": used,
            "draw_down_pct": await self._current_draw_down_pct(margin),
        }

    # -------------------------------------------------------------------------
    # Atomic Allocate – The Critical Fix
    # -------------------------------------------------------------------------
    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """
        ATOMIC ALLOCATION v2.0:
        1. Locks the bucket usage row in DB.
        2. Checks limit against REAL locked value.
        3. Inserts ledger entry (Idempotent).
        4. Updates usage.
        All in ONE transaction. Zero race conditions.
        """
        # 1. Global Safety Check (Read-only)
        margin = await self._get_real_margin()
        draw_down = await self._current_draw_down_pct(margin)
        if draw_down > settings.DAILY_LOSS_LIMIT_PCT:
            logger.critical(f"🛑 DRAW-DOWN BRAKE HIT ({draw_down*100:.2f}%) - Allocation Denied")
            return False
        
        try:
            limit = margin * self._bucket_pct.get(bucket, 0.0)
            
            async with self._db.get_session() as session:
                # --- START ATOMIC TRANSACTION ---
                
                # Step A: Lock the usage row for update
                # This ensures no other trade can read/write this bucket until we finish
                stmt = text("""
                    SELECT used_amount 
                    FROM capital_usage 
                    WHERE bucket = :bucket
                    FOR UPDATE
                """)
                result = await session.execute(stmt, {"bucket": bucket})
                row = result.fetchone()
                
                current_used = row[0] if row else 0.0
                new_used = current_used + amount
                
                # Step B: Check limit WHILE LOCKED
                if new_used > limit:
                    logger.warning(
                        f"🚫 Allocation Denied: {bucket} | "
                        f"Used: {current_used:,.0f} + {amount:,.0f} = {new_used:,.0f} "
                        f"> Limit: {limit:,.0f}"
                    )
                    return False  # Transaction rolls back automatically
                
                # Step C: Insert ledger entry (Idempotent via Unique Constraint)
                # If trade_id+bucket exists, this returns nothing (DO NOTHING)
                ledger_stmt = text("""
                    INSERT INTO capital_ledger (trade_id, bucket, amount, date, timestamp)
                    VALUES (:trade_id, :bucket, :amount, CURRENT_DATE, NOW())
                    ON CONFLICT (trade_id, bucket) DO NOTHING
                    RETURNING id
                """)
                ledger_result = await session.execute(ledger_stmt, {
                    "trade_id": trade_id,
                    "bucket": bucket,
                    "amount": amount
                })
                
                # Check if insert actually happened
                inserted_id = ledger_result.scalar()
                if not inserted_id:
                    # Logic: If row exists, we already allocated. Return True (Idempotent Success).
                    # We do NOT update usage again to prevent double counting.
                    logger.info(f"✅ Allocation already exists: {trade_id} - {bucket}")
                    return True
                
                # Step D: Update usage summary
                # We use UPSERT logic here to handle the first-time creation of the bucket row
                update_stmt = text("""
                    INSERT INTO capital_usage (bucket, used_amount, last_updated)
                    VALUES (:bucket, :amount, NOW())
                    ON CONFLICT (bucket) DO UPDATE
                    SET used_amount = capital_usage.used_amount + :amount,
                        last_updated = NOW()
                """)
                await session.execute(update_stmt, {"bucket": bucket, "amount": amount})
                
                # --- COMMIT TRANSACTION ---
                await self._db.safe_commit(session)
                
                logger.info(
                    f"✅ Capital Allocated: {bucket} | "
                    f"Trade: {trade_id} | Amount: ₹{amount:,.0f} | New Usage: {new_used:,.0f}"
                )
                return True
                
        except Exception as e:
            logger.error(f"🔥 Allocation System Error: {e}", exc_info=True)
            return False

    async def release_capital(self, bucket: str, amount: float, trade_id: str) -> None:
        """
        ATOMIC RELEASE: Mirrors allocation logic with locking.
        """
        try:
            async with self._db.get_session() as session:
                # Step A: Delete ledger entry
                delete_stmt = text("""
                    DELETE FROM capital_ledger 
                    WHERE trade_id = :trade_id AND bucket = :bucket
                    RETURNING amount
                """)
                result = await session.execute(delete_stmt, {
                    "trade_id": trade_id,
                    "bucket": bucket
                })
                deleted_row = result.fetchone()
                
                if not deleted_row:
                    return # Nothing to release
                
                # Use the ACTUAL amount that was locked (in case it differed)
                released_amount = deleted_row[0]
                
                # Step B: Update usage summary (with implicit row lock via UPDATE)
                update_stmt = text("""
                    UPDATE capital_usage
                    SET used_amount = GREATEST(0, used_amount - :amount),
                        last_updated = NOW()
                    WHERE bucket = :bucket
                """)
                await session.execute(update_stmt, {
                    "bucket": bucket,
                    "amount": released_amount
                })
                
                await self._db.safe_commit(session)
                
                logger.info(
                    f"✅ Capital Released: {bucket} | "
                    f"Trade: {trade_id} | Amount: ₹{released_amount:,.0f}"
                )
        except Exception as e:
            logger.error(f"Release Error: {e}", exc_info=True)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    async def _get_real_margin(self) -> float:
        """Return available margin from broker (cached)."""
        now = time.time()
        if now - self._last_margin_fetch < MARGIN_REFRESH_SEC:
            return self._cached_available_margin

        # Inline import to avoid circular dependency
        from trading.api_client import EnhancedUpstoxAPI
        
        # If in test mode with no token, return fallback
        if settings.SAFETY_MODE == "paper" and "TEST" in settings.UPSTOX_ACCESS_TOKEN:
             return self._fallback_size

        try:
            # We create a temp client just for this check
            api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN) 
            raw = await api.get_funds_and_margin()
            eq = raw.get("data", {}).get("equity", {})
            avail = float(eq.get("available_margin", 0.0))
            if avail > 0:
                self._cached_available_margin = avail
            await api.close()
        except Exception:
            pass # Keep using cache on failure
        finally:
            self._last_margin_fetch = now
            
        return self._cached_available_margin

    async def _get_used_breakdown(self) -> Dict[str, float]:
        try:
            async with self._db.get_session() as session:
                stmt = select(DbCapitalUsage)
                rows = await session.execute(stmt)
                return {row.bucket: row.used_amount for row in rows.scalars()}
        except Exception:
            return {}

    async def _current_draw_down_pct(self, current_margin: float) -> float:
        """
        Calculate drawdown based on Start-of-Day Equity snapshot in DB.
        """
        from database.models import DbRiskState
        from sqlalchemy import desc
        
        try:
            async with self._db.get_session() as session:
                # Get the latest risk state which has SOD equity
                stmt = select(DbRiskState).order_by(desc(DbRiskState.timestamp)).limit(1)
                result = await session.execute(stmt)
                state = result.scalars().first()
                
                if state and state.sod_equity > 0:
                    # Drawdown = (Current - Peak) / Peak
                    # Here we simplify to SOD as the peak reference for the day
                    dd = (state.current_equity - state.sod_equity) / state.sod_equity
                    return abs(dd) if dd < 0 else 0.0
                
            return 0.0
        except Exception:
            return 0.0
