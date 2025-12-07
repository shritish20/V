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
        """Get the total capital limit for a bucket"""
        return self.account_size * self.bucket_config.get(bucket, 0.0)

    async def allocate_capital(self, bucket: str, amount: float, trade_id: str) -> bool:
        """Allocate capital to a trade from a specific bucket"""
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
        """Release capital back to the bucket when trade closes"""
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

    def get_status(self) -> Dict[str, Dict[str, float]]:
        """
        ADDED METHOD: Returns current capital allocation status
        
        Returns:
            Dict with structure:
            {
                "available": {bucket: available_amount},
                "used": {bucket: used_amount},
                "limit": {bucket: limit_amount}
            }
        """
        status = {
            "available": {},
            "used": {},
            "limit": {}
        }
        
        for bucket in self.bucket_config.keys():
            limit = self.get_bucket_limit(bucket)
            used = sum(self.capital_used[bucket].values())
            status["available"][bucket] = limit - used
            status["used"][bucket] = used
            status["limit"][bucket] = limit
        
        return status

    def get_bucket_usage_pct(self, bucket: str) -> float:
        """Get the percentage of capital used in a bucket"""
        limit = self.get_bucket_limit(bucket)
        if limit == 0:
            return 0.0
        used = sum(self.capital_used[bucket].values())
        return (used / limit) * 100

    def get_total_usage(self) -> float:
        """Get total capital used across all buckets"""
        total_used = 0.0
        for bucket_trades in self.capital_used.values():
            total_used += sum(bucket_trades.values())
        return total_used

    def get_total_available(self) -> float:
        """Get total available capital across all buckets"""
        return self.account_size - self.get_total_usage()
