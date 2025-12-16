import logging
import asyncio
from typing import Dict, Optional
from sqlalchemy import select, update
from core.enums import CapitalBucket, StrategyType
from core.config import settings
from database.models import DbCapitalUsage

logger = logging.getLogger("SmartAllocator")

class SmartCapitalAllocator:
    def __init__(self, account_size: float, buckets: Dict, db_manager):
        self.account_size = account_size
        self.bucket_config = buckets
        self.db = db_manager

    def get_bucket_limit(self, bucket: str) -> float:
        return self.account_size * self.bucket_config.get(bucket, 0.0)

    def calculate_smart_lots(self, strategy: StrategyType, available_cap: float) -> int:
        """
        Calculates safe lot size based on Strategy Risk Profile.
        """
        # Estimated Margin per Lot
        margin_map = {
            StrategyType.IRON_CONDOR: 50000,
            StrategyType.IRON_FLY: 50000,
            StrategyType.JADE_LIZARD: 130000,
            StrategyType.SHORT_STRANGLE: 160000,
            StrategyType.RATIO_SPREAD_PUT: 180000
        }
        
        est_margin = margin_map.get(strategy, 150000)
        
        # Risk Weighting (0.0 - 1.0)
        risk_weight = 1.0
        if strategy in [StrategyType.SHORT_STRANGLE, StrategyType.RATIO_SPREAD_PUT]:
            risk_weight = 0.60 # Reduce size for undefined risk
            
        raw_lots = int(available_cap / est_margin)
        safe_lots = int(raw_lots * risk_weight)
        
        return max(1, min(safe_lots, settings.MAX_LOTS))

    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """Atomic DB Allocation"""
        limit = self.get_bucket_limit(bucket)
        
        async with self.db.get_session() as session:
            # Upsert row first to ensure it exists
            # (Simplified for brevity, assuming row exists via ensure_bucket_exists in startup)
            stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
            result = await session.execute(stmt)
            usage = result.scalar_one_or_none()
            
            if not usage:
                # Create if missing
                usage = DbCapitalUsage(bucket=bucket, used_amount=0.0)
                session.add(usage)
            
            if (usage.used_amount + amount) > limit:
                logger.warning(f"💰 Allocation Denied: {bucket} (Req {amount:,.0f} > Limit)")
                return False
                
            usage.used_amount += amount
            await self.db.safe_commit(session)
            logger.info(f"💰 Allocated ₹{amount:,.0f} to {trade_id}")
            return True

    async def release_capital(self, bucket: str, trade_id: str, amount: float):
        if amount <= 0: return
        
        async with self.db.get_session() as session:
            stmt = select(DbCapitalUsage).where(DbCapitalUsage.bucket == bucket).with_for_update()
            result = await session.execute(stmt)
            usage = result.scalar_one_or_none()
            
            if usage:
                usage.used_amount = max(0.0, usage.used_amount - amount)
                await self.db.safe_commit(session)
                logger.info(f"♻️ Released ₹{amount:,.0f} from {trade_id}")

    async def get_status(self) -> Dict:
        status = {"available": {}, "used": {}, "limit": {}}
        async with self.db.get_session() as session:
            result = await session.execute(select(DbCapitalUsage))
            rows = result.scalars().all()
            
            db_map = {r.bucket: r.used_amount for r in rows}
            
            for b in self.bucket_config:
                limit = self.get_bucket_limit(b)
                used = db_map.get(b, 0.0)
                status["limit"][b] = limit
                status["used"][b] = used
                status["available"][b] = limit - used
        return status
