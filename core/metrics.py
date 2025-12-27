#!/usr/bin/env python3
"""
VolGuard 20.0 – Simple Metrics Store (No Prometheus)
Metrics stored in-memory and exposed via REST API
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List
from collections import deque
import asyncio

@dataclass
class SystemMetrics:
    """Live metrics for React dashboard"""
    # Counters (Reset daily)
    stale_data_count: int = 0
    capital_allocation_success: int = 0
    capital_allocation_failed: int = 0
    trades_executed: int = 0
    trades_rejected: int = 0
    rollback_attempts: int = 0
    
    # Gauges (Current values)
    active_positions: int = 0
    total_capital_used: float = 0.0
    current_pnl: float = 0.0
    
    # Time-series (Last 100 events)
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=100))
    recent_trades: deque = field(default_factory=lambda: deque(maxlen=50))
    
    # Timestamps
    last_reset: datetime = field(default_factory=datetime.utcnow)
    last_stale_data: datetime = None
    last_trade: datetime = None
    
    def reset_daily_counters(self):
        """Called at market open"""
        self.stale_data_count = 0
        self.capital_allocation_success = 0
        self.capital_allocation_failed = 0
        self.trades_executed = 0
        self.trades_rejected = 0
        self.rollback_attempts = 0
        self.last_reset = datetime.utcnow()
    
    def log_stale_data(self, instrument: str):
        """Track stale data events"""
        self.stale_data_count += 1
        self.last_stale_data = datetime.utcnow()
        self.recent_errors.append({
            "type": "stale_data",
            "instrument": instrument,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def log_allocation(self, success: bool, bucket: str, amount: float, trade_id: str):
        """Track capital allocations"""
        if success:
            self.capital_allocation_success += 1
        else:
            self.capital_allocation_failed += 1
            self.recent_errors.append({
                "type": "allocation_failed",
                "bucket": bucket,
                "amount": amount,
                "trade_id": trade_id,
                "timestamp": datetime.utcnow().isoformat()
            })
    
    def log_trade(self, success: bool, trade_id: str, strategy: str, reason: str = None):
        """Track trade execution"""
        if success:
            self.trades_executed += 1
            self.last_trade = datetime.utcnow()
            self.recent_trades.append({
                "trade_id": trade_id,
                "strategy": strategy,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "executed"
            })
        else:
            self.trades_rejected += 1
            self.recent_errors.append({
                "type": "trade_rejected",
                "trade_id": trade_id,
                "strategy": strategy,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            })
    
    def log_rollback(self, trade_id: str, legs_count: int, success: bool):
        """Track rollback attempts (CRITICAL)"""
        self.rollback_attempts += 1
        self.recent_errors.append({
            "type": "rollback",
            "trade_id": trade_id,
            "legs_count": legs_count,
            "success": success,
            "timestamp": datetime.utcnow().isoformat(),
            "severity": "CRITICAL"
        })
    
    def update_gauges(self, positions: int, capital: float, pnl: float):
        """Update current state"""
        self.active_positions = positions
        self.total_capital_used = capital
        self.current_pnl = pnl
    
    def to_dict(self) -> Dict:
        """Serialize for API response"""
        return {
            "counters": {
                "stale_data": self.stale_data_count,
                "allocations_success": self.capital_allocation_success,
                "allocations_failed": self.capital_allocation_failed,
                "trades_executed": self.trades_executed,
                "trades_rejected": self.trades_rejected,
                "rollback_attempts": self.rollback_attempts,
            },
            "gauges": {
                "active_positions": self.active_positions,
                "total_capital_used": self.total_capital_used,
                "current_pnl": self.current_pnl,
            },
            "timestamps": {
                "last_reset": self.last_reset.isoformat() if self.last_reset else None,
                "last_stale_data": self.last_stale_data.isoformat() if self.last_stale_data else None,
                "last_trade": self.last_trade.isoformat() if self.last_trade else None,
            },
            "recent_errors": list(self.recent_errors)[-10:],  # Last 10 only
            "recent_trades": list(self.recent_trades)[-10:],
        }

# Global instance
_metrics = SystemMetrics()

def get_metrics() -> SystemMetrics:
    """Get global metrics instance"""
    return _metrics
