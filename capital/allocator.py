#!/usr/bin/env python3
"""
SmartCapitalAllocator 20.0 – Production Hardened
- Real margin from broker (funds_margin)
- Idempotent allocate / release per trade-id (Prevents Double Counting)
- Ledger draw-down brake (Stops trading if daily loss limit hit)
- Thread-safe
"""
from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
from core.enums import CapitalBucket
from core.config import settings
from database.models import DbCapitalUsage, DbCapitalLedger

logger = logging.getLogger("CapitalAllocator")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MARGIN_REFRESH_SEC = 30  # Broker margin cache TTL (prevents API spam)

# ---------------------------------------------------------------------------
# Allocator
# ---------------------------------------------------------------------------
class SmartCapitalAllocator:
    def __init__(self, fallback_account_size: float, allocation_config: Dict[str, float], db) -> None:
        self._fallback_size = fallback_account_size
        self._bucket_pct = allocation_config
        self._db = db
        self._lock = asyncio.Lock()
        self._last_margin_fetch = 0.0
        self._cached_available_margin = fallback_account_size

    # -------------------------------------------------------------------------
    # Public – Used by Engine every cycle
    # -------------------------------------------------------------------------
    async def get_status(self) -> Dict[str, Any]:
        """Return current capital status (includes real margin)."""
        margin = await self._get_real_margin()
        used = await self._get_used_breakdown()
        
        buckets = {
            bucket: {
                "limit": margin * self._bucket_pct.get(bucket, 0.0),
                "used": used.get(bucket, 0.0),
                "avail": (margin * self._bucket_pct.get(bucket, 0.0)) - used.get(bucket, 0.0),
            }
            for bucket in self._bucket_pct
        }
        
        return {
            "total": margin,
            "cached_margin": margin,
            "buckets": buckets,
            "used": used,
            "draw_down_pct": await self._current_draw_down_pct(margin),
        }

    # -------------------------------------------------------------------------
    # Allocate – Idempotent
    # -------------------------------------------------------------------------
    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """
        Idempotent allocate.
        Returns True if *this* trade_id was successfully allocated (or already booked).
        """
        async with self._lock:
            # 1. Duplicate Check (Idempotency)
            if await self._already_allocated(trade_id):
                logger.info("⚡ Trade already allocated – skipping", extra={"trade_id": trade_id})
                return True

            # 2. Draw-down Brake (Circuit Breaker)
            margin = await self._get_real_margin()
            draw_down = await self._current_draw_down_pct(margin)
            
            if draw_down > settings.DAILY_LOSS_LIMIT_PCT:
                logger.critical(
                    "🛑 DRAW-DOWN BRAKE HIT – ALLOCATION REFUSED",
                    extra={"draw_down_pct": round(draw_down * 100, 2), "limit_pct": settings.DAILY_LOSS_LIMIT_PCT * 100},
                )
                return False

            # 3. Bucket Limit Check
            used = await self._get_used_breakdown()
            bucket_limit = margin * self._bucket_pct.get(bucket, 0.0)
            
            if used.get(bucket, 0.0) + amount > bucket_limit:
                logger.warning(
                    "⚠️ Bucket limit exceeded",
                    extra={
                        "bucket": bucket,
                        "requested": amount,
                        "avail": bucket_limit - used.get(bucket, 0.0),
                    },
                )
                return False

            # 4. Book the Capital
            await self._book_allocate(bucket, amount, trade_id)
            logger.info(
                f"💰 Capital Allocated: ₹{amount:,.0f} | {bucket}",
                extra={
                    "trade_id": trade_id,
                    "margin_left": margin - used.get(bucket, 0.0) - amount,
                },
            )
            return True

    # -------------------------------------------------------------------------
    # Release – Idempotent
    # -------------------------------------------------------------------------
    async def release_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """
        Idempotent release.
        Returns True if *this* trade_id was successfully released (or already released).
        """
        async with self._lock:
            if not await self._already_allocated(trade_id):
                logger.info("Trade not found in ledger – nothing to release", extra={"trade_id": trade_id})
                return True

            await self._book_release(bucket, amount, trade_id)
            logger.info(
                f"♻️ Capital Released: ₹{amount:,.0f}",
                extra={"trade_id": trade_id, "bucket": bucket},
            )
            return True

    # -------------------------------------------------------------------------
    # Real Margin – Cached 30s
    # -------------------------------------------------------------------------
    async def _get_real_margin(self) -> float:
        """Return available margin from broker (cached)."""
        now = time.time()
        if now - self._last_margin_fetch < MARGIN_REFRESH_SEC:
            return self._cached_available_margin

        # Fetch from broker
        # We perform inline import to avoid circular dependency with Engine
        from trading.api_client import EnhancedUpstoxAPI, MarginInsaneError

        api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN) 
        try:
            raw = await api.get_funds_and_margin()
            eq = raw.get("data", {}).get("equity", {})
            avail = float(eq.get("available_margin", 0.0))
            
            # Sanity Check handled by API, but double check here
            if avail <= 0:
                logger.critical(f"🔥 BROKER MARGIN IS NEGATIVE OR ZERO: {avail}")
                
            self._cached_available_margin = avail
            await api.close() # Clean up temporary connection
        except Exception as exc:
            logger.warning("⚠️ Margin fetch failed – using fallback/cache", exc_info=exc)
            # We keep using the old cached value or fallback if cache is empty
            if self._cached_available_margin <= 0:
                self._cached_available_margin = self._fallback_size
        finally:
            self._last_margin_fetch = now
        return self._cached_available_margin

    # -------------------------------------------------------------------------
    # Draw-down on Ledger Balance
    # -------------------------------------------------------------------------
    async def _current_draw_down_pct(self, current_margin: float) -> float:
        """
        Draw-down = (start_of_day_margin - current_margin) / start_of_day_margin
        start_of_day_margin is stored in DbCapitalLedger at 09:15
        """
        async with self._db.get_session() as session:
            # Check if we have a Start-Of-Day (SOD) entry for today
            row = await session.scalar(
                select(DbCapitalLedger)
                .where(DbCapitalLedger.trade_id == "SOD")
                .where(DbCapitalLedger.date == datetime.now(settings.IST).date())
            )
            
            if not row or row.amount <= 0:
                # First call of the day – Seed the SOD balance
                logger.info(f"🌅 Seeding Start-of-Day Balance: ₹{current_margin:,.0f}")
                row = DbCapitalLedger(
                    bucket="START_OF_DAY",
                    amount=current_margin,
                    date=datetime.now(settings.IST).date(),
                    trade_id="SOD",
                )
                session.add(row)
                await self._db.safe_commit(session)
                return 0.0

            sod = row.amount
            if sod <= 0: return 0.0
            
            # Calculate Drawdown
            dd = max(0.0, (sod - current_margin) / sod)
            return dd

    # -------------------------------------------------------------------------
    # Idempotent Book-keeping
    # -------------------------------------------------------------------------
    async def _already_allocated(self, trade_id: str) -> bool:
        async with self._db.get_session() as session:
            row = await session.scalar(
                select(DbCapitalLedger).where(DbCapitalLedger.trade_id == trade_id)
            )
            return row is not None

    async def _get_used_breakdown(self) -> Dict[str, float]:
        async with self._db.get_session() as session:
            rows = await session.execute(select(DbCapitalUsage))
            return {row.bucket: row.used_amount for row in rows.scalars()}

    async def _book_allocate(self, bucket: str, amount: float, trade_id: str) -> None:
        async with self._db.get_session() as session:
            # 1. Upsert Usage
            stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
            row = await session.scalar(stmt)
            if not row:
                row = DbCapitalUsage(bucket=bucket, used_amount=0.0)
                session.add(row)
            row.used_amount += amount
            row.last_updated = datetime.now(settings.IST)

            # 2. Add to Ledger (Idempotency Key)
            ledger = DbCapitalLedger(
                bucket=bucket,
                amount=amount,
                date=datetime.now(settings.IST).date(),
                trade_id=trade_id,
            )
            session.add(ledger)
            await self._db.safe_commit(session)

    async def _book_release(self, bucket: str, amount: float, trade_id: str) -> None:
        async with self._db.get_session() as session:
            # 1. Update Usage
            stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
            row = await session.scalar(stmt)
            if row:
                row.used_amount = max(0.0, row.used_amount - amount)
                row.last_updated = datetime.now(settings.IST)

            # 2. Remove from Ledger
            await session.execute(
                delete(DbCapitalLedger).where(DbCapitalLedger.trade_id == trade_id)
            )
            await self._db.safe_commit(session)
