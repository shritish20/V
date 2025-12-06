from typing import Dict
from utils.logger import setup_logger

logger = setup_logger("CapitalAllocator")

class SmartCapitalAllocator:
    def __init__(self, account_size: float, buckets: Dict[str, float]):
        self.account_size = account_size
        self.bucket_config = buckets
        # capital_used[bucket] = {trade_id: amount}
        self.capital_used: Dict[str, Dict[str, float]] = {
            k: {} for k in buckets.keys()
        }

    def get_bucket_limit(self, bucket: str) -> float:
        return self.account_size * self.bucket_config.get(bucket, 0.0)

    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        used = sum(self.capital_used[bucket].values())
        limit = self.get_bucket_limit(bucket)
        
        if used + amount > limit:
            logger.warning(
                f"🚫 Capital Allocation Denied: bucket={bucket} used={used:.0f} "
                f"req={amount:.0f} limit={limit:.0f}"
            )
            return False
        
        self.capital_used[bucket][trade_id] = amount
        logger.info(
            f"✅ Capital Allocated: bucket={bucket} trade={trade_id} amount={amount:.0f}"
        )
        return True

    async def release_capital(self, bucket: str, trade_id: str):
        bucket_map = self.capital_used.get(bucket, {})
        if trade_id in bucket_map:
            freed = bucket_map.pop(trade_id)
            logger.info(
                f"💸 Capital Released: bucket={bucket} trade={trade_id} amount={freed:.0f}"
            )
        else:
            logger.debug(
                f"release_capital: no entry for bucket={bucket} trade={trade_id}"
            )
