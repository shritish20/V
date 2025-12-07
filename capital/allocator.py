import asyncio
from typing import Dict
from utils.logger import setup_logger

logger = setup_logger("CapitalAllocator")

class SmartCapitalAllocator:
    def __init__(self, account_size: float, buckets: Dict[str, float]):
        self.account_size = account_size
        self.bucket_config = buckets
        self.capital_used: Dict[str, Dict[str, float]] = {k: {} for k in buckets.keys()}
        
        # FIX: Async Locks for Thread Safety (Logic safety in async loop)
        self._locks: Dict[str, asyncio.Lock] = {k: asyncio.Lock() for k in buckets.keys()}

    def get_bucket_limit(self, bucket: str) -> float:
        return self.account_size * self.bucket_config.get(bucket, 0.0)

    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        if bucket not in self._locks: return False
        
        async with self._locks[bucket]:
            used = sum(self.capital_used[bucket].values())
            limit = self.get_bucket_limit(bucket)

            if used + amount > limit:
                logger.warning(f"🚫 Capital Denied: {bucket} (Req: {amount:.0f}, Avail: {limit-used:.0f})")
                return False

            self.capital_used[bucket][trade_id] = amount
            logger.info(f"💰 Allocated {amount:.0f} to {trade_id} ({bucket})")
            return True

    async def release_capital(self, bucket: str, trade_id: str):
        if bucket not in self._locks: return
        
        async with self._locks[bucket]:
            bucket_map = self.capital_used.get(bucket, {})
            if trade_id in bucket_map:
                freed = bucket_map.pop(trade_id)
                logger.info(f"💸 Released {freed:.0f} from {trade_id}")

    def get_status(self) -> Dict[str, Dict[str, float]]:
        status = {"available": {}, "used": {}, "limit": {}}
        for bucket in self.bucket_config.keys():
            limit = self.get_bucket_limit(bucket)
            used = sum(self.capital_used[bucket].values())
            status["available"][bucket] = limit - used
            status["used"][bucket] = used
            status["limit"][bucket] = limit
        return status
