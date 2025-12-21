from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from database.models import DbCapitalUsage
from core.enums import CapitalBucket
from core.config import settings
import logging

logger = logging.getLogger("CapitalAllocator")

class SmartCapitalAllocator:
    def __init__(self, total_account_size: float, allocation_config: dict, db_manager):
        self.total_size = total_account_size
        self.bucket_config = allocation_config
        self.db = db_manager

    async def get_status(self):
        async with self.db.get_session() as session:
            result = await session.execute(select(DbCapitalUsage))
            usage = {row.bucket: row.used_amount for row in result.scalars().all()}
            
            status = {"total": self.total_size, "buckets": {}, "used": {}}
            for bucket, pct in self.bucket_config.items():
                limit = self.total_size * pct
                used = usage.get(bucket, 0.0)
                status["buckets"][bucket] = limit
                status["used"][bucket] = used
            return status

    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """
        Thread-safe capital allocation using Row Locking.
        """
        limit = self.total_size * self.bucket_config.get(bucket, 0.0)
        
        async with self.db.get_session() as session:
            try:
                # 1. Ensure Row Exists (Atomic Insert if not exists)
                # We try to fetch first
                stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
                result = await session.execute(stmt)
                row = result.scalar_one_or_none()
                
                if not row:
                    # Create row if missing
                    try:
                        new_row = DbCapitalUsage(bucket=bucket, used_amount=0.0)
                        session.add(new_row)
                        await session.flush() # Force ID generation/Insert
                        row = new_row
                    except IntegrityError:
                        # Race condition caught: another thread inserted it. Retry select.
                        await session.rollback()
                        result = await session.execute(stmt)
                        row = result.scalar_one()

                # 2. Check & Allocate
                if row.used_amount + amount <= limit:
                    row.used_amount += amount
                    row.last_updated = settings.datetime.now()
                    await self.db.safe_commit(session)
                    logger.info(f"💰 Allocated ₹{amount:,.0f} from {bucket} for {trade_id}")
                    return True
                else:
                    logger.warning(f"🚫 Capital Reject: {bucket} (Req: {amount}, Avail: {limit - row.used_amount})")
                    return False
                    
            except Exception as e:
                logger.error(f"Allocation Error: {e}")
                await session.rollback()
                return False

    async def release_capital(self, bucket: str, amount: float):
        async with self.db.get_session() as session:
            stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            
            if row:
                row.used_amount = max(0.0, row.used_amount - amount)
                await self.db.safe_commit(session)
                logger.info(f"♻️ Released ₹{amount:,.0f} to {bucket}")
