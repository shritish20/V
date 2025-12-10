import asyncio
from typing import Dict, Optional
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

from core.config import settings
from utils.logger import setup_logger
from database.manager import HybridDatabaseManager
from database.models import DbCapitalUsage

logger = setup_logger("CapitalAllocator")

class SmartCapitalAllocator:
    """
    Institutional Capital Allocator v2.0
    Uses Database Row-Locking (SELECT ... FOR UPDATE) to prevent race conditions.
    Stateless architecture: Safe for restarts and scaling.
    """
    def __init__(self, account_size: float, buckets: Dict[str, float], db_manager: HybridDatabaseManager):
        self.account_size = account_size
        self.bucket_config = buckets
        self.db = db_manager
        # No local cache: State is now strictly in the DB

    def get_bucket_limit(self, bucket: str) -> float:
        return self.account_size * self.bucket_config.get(bucket, 0.0)

    async def _ensure_bucket_exists(self, session, bucket: str):
        """Ensures the bucket row exists in DB to allow locking."""
        # This handles the first-run scenario where the row might not exist yet
        stmt = insert(DbCapitalUsage).values(bucket=bucket, used_amount=0.0).on_conflict_do_nothing()
        await session.execute(stmt)

    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """
        ATOMIC ALLOCATION: Locks the DB row, checks limit, updates balance.
        """
        if amount <= 0: return False
        
        limit = self.get_bucket_limit(bucket)

        try:
            async with self.db.get_session() as session:
                # 1. Ensure row exists so we have something to lock
                await self._ensure_bucket_exists(session, bucket)
                
                # 2. LOCK the row (SELECT ... FOR UPDATE)
                # This freezes this specific bucket row. No other trade can read/write 
                # to this bucket until this transaction commits or rolls back.
                stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
                result = await session.execute(stmt)
                usage_record = result.scalar_one()

                # 3. Check Logic
                current_used = usage_record.used_amount
                
                if (current_used + amount) > limit:
                    logger.warning(
                        f"🚫 Capital Denied for {bucket} | "
                        f"Req: {amount:,.0f} | Avail: {limit - current_used:,.0f}"
                    )
                    # Implicit rollback happens on exit if no commit
                    return False

                # 4. Update and Commit
                usage_record.used_amount += amount
                await self.db.safe_commit(session)
                
                logger.info(f"💰 Allocated {amount:,.0f} to {trade_id} ({bucket})")
                return True

        except Exception as e:
            logger.error(f"Allocation Error: {e}")
            return False

    async def release_capital(self, bucket: str, trade_id: str, amount: Optional[float] = None):
        """
        Releases capital. 
        NOTE: In this stateless version, we REQUIRE the exact amount to release.
        """
        if amount is None or amount <= 0:
            logger.warning(f"⚠️ Release ignored: Invalid amount {amount} for {trade_id}")
            return

        try:
            async with self.db.get_session() as session:
                # 1. Lock the row to safely deduct
                stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
                result = await session.execute(stmt)
                usage_record = result.scalar_one_or_none()

                if usage_record:
                    # 2. Deduct
                    new_amount = max(0.0, usage_record.used_amount - amount)
                    usage_record.used_amount = new_amount
                    await self.db.safe_commit(session)
                    logger.info(f"💸 Released {amount:,.0f} from {trade_id} | Bucket: {bucket}")
                else:
                    logger.warning(f"⚠️ Bucket {bucket} not found during release")

        except Exception as e:
            logger.error(f"Capital Release Error: {e}")

    async def get_status(self) -> Dict[str, Dict[str, float]]:
        """Fetches live status from DB."""
        status = {"available": {}, "used": {}, "limit": {}}
        
        try:
            async with self.db.get_session() as session:
                # No lock needed for simple status check (read-only)
                result = await session.execute(select(DbCapitalUsage))
                rows = result.scalars().all()
                db_map = {r.bucket: r.used_amount for r in rows}

            for bucket in self.bucket_config.keys():
                limit = self.get_bucket_limit(bucket)
                used = db_map.get(bucket, 0.0)
                
                status["available"][bucket] = limit - used
                status["used"][bucket] = used
                status["limit"][bucket] = limit
                
            return status
        except Exception as e:
            logger.error(f"Status Fetch Error: {e}")
            # Return empty structure on error to prevent crashes downstream
            return {"available": {}, "used": {}, "limit": {}}
