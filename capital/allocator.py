import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from core.config import settings, IST
from core.enums import CapitalBucket, ExpiryType

logger = logging.getLogger("VolGuard18")

@dataclass
class AllocationRecord:
    timestamp: datetime
    bucket: CapitalBucket
    amount: float
    trade_id: Optional[int] = None
    description: str = ""
    instrument_key: Optional[str] = None

class SmartCapitalAllocator:
    def __init__(self, total_capital: float, allocation_config: Dict[str, float]):
        self.total_capital = total_capital
        self.allocation_config = allocation_config
        self._validate_allocation_config()

        self.allocated_capital: Dict[str, float] = {
            bucket: total_capital * allocation_config.get(bucket, 0)
            for bucket in allocation_config.keys()
        }

        self.used_capital: Dict[str, float] = {bucket: 0.0 for bucket in allocation_config.keys()}
        self.reserved_capital: Dict[str, float] = {bucket: 0.0 for bucket in allocation_config.keys()}
        self.allocation_history: List[AllocationRecord] = []
        self.bucket_performance: Dict[str, Dict[str, float]] = {
            bucket: {
                "total_allocated": self.allocated_capital.get(bucket, 0),
                "total_used": 0.0,
                "total_pnl": 0.0,
                "win_count": 0,
                "loss_count": 0
            }
            for bucket in allocation_config.keys()
        }
        logger.info(f"💰 Capital Allocator initialized with total: ₹{total_capital:,.0f}")
        logger.info(f"📊 Allocation: {allocation_config}")

    def _validate_allocation_config(self):
        total = sum(self.allocation_config.values())
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Capital allocation must total 100%, got {total * 100:.1f}%")
        for bucket, pct in self.allocation_config.items():
            if pct < 0 or pct > 1:
                raise ValueError(f"Invalid percentage for {bucket}: {pct}")
            if bucket not in [b.value for b in CapitalBucket]:
                logger.warning(f"Unknown capital bucket: {bucket}")

    def allocate_capital(self, bucket: str, amount: float, trade_id: Optional[int] = None,
                         description: str = "", instrument_key: Optional[str] = None) -> bool:
        if bucket not in self.used_capital:
            logger.error(f"Invalid capital bucket: {bucket}")
            return False

        new_total = self.used_capital[bucket] + self.reserved_capital[bucket] + amount
        bucket_limit = self.allocated_capital[bucket]
        if new_total > bucket_limit:
            logger.warning(f"Cannot allocate ₹{amount:,.0f} to {bucket}. "
                           f"Used: {self.used_capital[bucket]:,.0f}, "
                           f"Reserved: {self.reserved_capital[bucket]:,.0f}, "
                           f"Limit: {bucket_limit:,.0f}")
            return False

        self.used_capital[bucket] += amount
        self.bucket_performance[bucket]["total_used"] += amount

        record = AllocationRecord(
            timestamp=datetime.now(IST),
            bucket=CapitalBucket(bucket),
            amount=amount,
            trade_id=trade_id,
            description=description,
            instrument_key=instrument_key
        )
        self.allocation_history.append(record)
        logger.debug(f"Allocated ₹{amount:,.0f} to {bucket}. "
                     f"Used: {self.used_capital[bucket]:,.0f}/{self.allocated_capital[bucket]:,.0f}")
        return True

    def reserve_capital(self, bucket: str, amount: float, trade_id: Optional[int] = None) -> bool:
        if bucket not in self.reserved_capital:
            logger.error(f"Invalid capital bucket: {bucket}")
            return False

        available = self.get_available_capital(bucket)
        if amount > available:
            logger.warning(f"Cannot reserve ₹{amount:,.0f} from {bucket}. Available: {available:,.0f}")
            return False

        self.reserved_capital[bucket] += amount
        logger.debug(f"Reserved ₹{amount:,.0f} from {bucket}. Reserved: {self.reserved_capital[bucket]:,.0f}")
        return True

    def release_reserved_capital(self, bucket: str, amount: float) -> bool:
        if bucket not in self.reserved_capital:
            logger.error(f"Invalid capital bucket: {bucket}")
            return False
        if amount > self.reserved_capital[bucket]:
            logger.warning(f"Cannot release ₹{amount:,.0f} from {bucket}. "
                           f"Only ₹{self.reserved_capital[bucket]:,.0f} is reserved")
            return False

        self.reserved_capital[bucket] -= amount
        logger.debug(f"Released ₹{amount:,.0f} from {bucket} reserves. "
                     f"Reserved: {self.reserved_capital[bucket]:,.0f}")
        return True

    def release_capital(self, bucket: str, amount: float, trade_id: Optional[int] = None) -> bool:
        if bucket not in self.used_capital:
            logger.error(f"Invalid capital bucket: {bucket}")
            return False
        if amount > self.used_capital[bucket]:
            logger.warning(f"Cannot release ₹{amount:,.0f} from {bucket}. "
                           f"Only ₹{self.used_capital[bucket]:,.0f} is allocated")
            return False

        self.used_capital[bucket] -= amount
        self.bucket_performance[bucket]["total_used"] -= amount
        logger.debug(f"Released ₹{amount:,.0f} from {bucket}. "
                     f"Used: {self.used_capital[bucket]:,.0f}/{self.allocated_capital[bucket]:,.0f}")
        return True

    def get_available_capital(self, bucket: str) -> float:
        if bucket not in self.allocated_capital:
            return 0.0
        allocated = self.allocated_capital[bucket]
        used = self.used_capital[bucket]
        reserved = self.reserved_capital[bucket]
        return max(0, allocated - used - reserved)

    def get_usage_percentage(self, bucket: str) -> float:
        if bucket not in self.allocated_capital or self.allocated_capital[bucket] == 0:
            return 0.0
        return ((self.used_capital[bucket] + self.reserved_capital[bucket]) /
                self.allocated_capital[bucket]) * 100

    def get_total_used_capital(self) -> float:
        return sum(self.used_capital.values()) + sum(self.reserved_capital.values())

    def get_total_available_capital(self) -> float:
        total_allocated = sum(self.allocated_capital.values())
        total_used = sum(self.used_capital.values())
        total_reserved = sum(self.reserved_capital.values())
        return max(0, total_allocated - total_used - total_reserved)

    def get_allocation_status(self) -> Dict[str, Dict[str, float]]:
        status = {
            "allocated": self.allocated_capital.copy(),
            "used": self.used_capital.copy(),
            "reserved": self.reserved_capital.copy(),
            "available": {},
            "usage_percentage": {},
            "performance": self.bucket_performance.copy()
        }
        for bucket in self.allocated_capital.keys():
            status["available"][bucket] = self.get_available_capital(bucket)
            status["usage_percentage"][bucket] = self.get_usage_percentage(bucket)
        return status

    def reset_allocation(self):
        self.used_capital = {bucket: 0.0 for bucket in self.allocated_capital.keys()}
        self.reserved_capital = {bucket: 0.0 for bucket in self.allocated_capital.keys()}
        logger.info("Capital allocation reset")

    def adjust_allocation(self, new_allocation: Dict[str, float]) -> bool:
        total = sum(new_allocation.values())
        if abs(total - 1.0) > 0.01:
            logger.error(f"Invalid allocation: percentages sum to {total}, should be 1.0")
            return False

        total_used = self.get_total_used_capital()
        min_bucket_pct = min(new_allocation.values())
        min_bucket_capacity = self.total_capital * min_bucket_pct
        if total_used > min_bucket_capacity:
            logger.error("Cannot adjust allocation: too much capital is in use")
            return False

        self.allocation_config = new_allocation
        self.allocated_capital = {
            bucket: self.total_capital * pct for bucket, pct in new_allocation.items()
        }
        logger.info(f"Capital allocation adjusted to: {new_allocation}")
        return True

    def get_bucket_for_expiry_type(self, expiry_type: ExpiryType) -> CapitalBucket:
        if expiry_type == ExpiryType.WEEKLY:
            return CapitalBucket.WEEKLY
        elif expiry_type == ExpiryType.MONTHLY:
            return CapitalBucket.MONTHLY
        else:
            return CapitalBucket.INTRADAY

    def can_allocate_for_expiry(self, expiry_type: ExpiryType, amount: float) -> bool:
        bucket = self.get_bucket_for_expiry_type(expiry_type)
        return self.get_available_capital(bucket.value) >= amount

    def update_performance(self, bucket: str, pnl: float, is_win: bool = True):
        if bucket not in self.bucket_performance:
            return
        self.bucket_performance[bucket]["total_pnl"] += pnl
        if is_win:
            self.bucket_performance[bucket]["win_count"] += 1
        else:
            self.bucket_performance[bucket]["loss_count"] += 1

    def get_bucket_performance(self, bucket: str) -> Dict[str, float]:
        if bucket not in self.bucket_performance:
            return {}
        perf = self.bucket_performance[bucket]
        total_trades = perf["win_count"] + perf["loss_count"]
        return {
            "total_allocated": self.allocated_capital.get(bucket, 0),
            "total_used": perf["total_used"],
            "total_pnl": perf["total_pnl"],
            "win_count": perf["win_count"],
            "loss_count": perf["loss_count"],
            "total_trades": total_trades,
            "win_rate": (perf["win_count"] / total_trades * 100) if total_trades > 0 else 0,
            "roi": (perf["total_pnl"] / perf["total_used"] * 100) if perf["total_used"] > 0 else 0
        }

    def get_allocation_history(self, limit: int = 100) -> List[Dict]:
        return [
            {
                "timestamp": record.timestamp.isoformat(),
                "bucket": record.bucket.value,
                "amount": record.amount,
                "trade_id": record.trade_id,
                "description": record.description,
                "instrument_key": record.instrument_key
            }
            for record in self.allocation_history[-limit:]
        ]

    def clear_history(self):
        self.allocation_history.clear()
        logger.debug("Allocation history cleared")
