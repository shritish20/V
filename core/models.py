from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum
from core.config import settings, IST
from core.enums import *

# ==================== TRADING MODELS ====================

@dataclass
class GreeksSnapshot:
    timestamp: datetime
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0
    pop: float = 0.0
    charm: float = 0.0
    vanna: float = 0.0

    def is_stale(self, max_age: float = 30.0) -> bool:
        return (datetime.now(IST) - self.timestamp).total_seconds() > max_age

    def to_dict(self) -> Dict[str, float]:
        return {
            'delta': self.delta, 'gamma': self.gamma, 'theta': self.theta,
            'vega': self.vega, 'iv': self.iv, 'pop': self.pop,
            'timestamp': self.timestamp.isoformat()
        }

class Position(BaseModel):
    symbol: str
    instrument_key: str
    strike: float
    option_type: str  # CE or PE
    quantity: int
    entry_price: float
    entry_time: datetime
    current_price: float
    current_greeks: GreeksSnapshot
    transaction_costs: float = 0.0
    expiry_type: ExpiryType = ExpiryType.WEEKLY
    capital_bucket: CapitalBucket = CapitalBucket.WEEKLY
    tags: List[str] = Field(default_factory=list)

    @validator('option_type')
    def validate_option_type(cls, v):
        if v not in ['CE', 'PE']:
            raise ValueError('option_type must be either "CE" or "PE"')
        return v

    def unrealized_pnl(self) -> float:
        return (self.current_price - self.entry_price) * self.quantity

    def update_price(self, new_price: float):
        self.current_price = new_price

class MultiLegTrade(BaseModel):
    legs: List[Position]
    strategy_type: StrategyType
    net_premium_per_share: float
    entry_time: datetime
    lots: int = 1
    status: TradeStatus = TradeStatus.OPEN
    expiry_date: str
    expiry_type: ExpiryType
    capital_bucket: CapitalBucket
    
    # Risk Limits
    max_loss_per_lot: float = 0.0
    max_profit_per_lot: float = 0.0
    breakeven_lower: float = 0.0
    breakeven_upper: float = 0.0
    transaction_costs: float = 0.0
    
    # Execution Details
    basket_order_id: Optional[str] = None
    gtt_order_ids: List[str] = Field(default_factory=list)
    id: Optional[str] = None
    exit_reason: Optional[ExitReason] = None

    # Portfolio Greeks
    trade_vega: float = 0.0
    trade_delta: float = 0.0
    trade_theta: float = 0.0
    trade_gamma: float = 0.0

    def total_unrealized_pnl(self) -> float:
        return sum(leg.unrealized_pnl() for leg in self.legs) - self.transaction_costs

    def calculate_trade_greeks(self):
        self.trade_delta = sum((leg.current_greeks.delta or 0) * leg.quantity for leg in self.legs)
        self.trade_gamma = sum((leg.current_greeks.gamma or 0) * leg.quantity for leg in self.legs)
        self.trade_theta = sum((leg.current_greeks.theta or 0) * leg.quantity for leg in self.legs)
        self.trade_vega = sum((leg.current_greeks.vega or 0) * leg.quantity for leg in self.legs)

    def calculate_max_loss(self):
        # Placeholder for complex spread calculations
        self.max_loss_per_lot = self.net_premium_per_share 

    def calculate_max_profit(self):
        self.max_profit_per_lot = self.net_premium_per_share

    def calculate_breakevens(self):
        self.breakeven_lower = 0.0
        self.breakeven_upper = 0.0

# ==================== TERMINAL / MANUAL REQUEST MODELS ====================

class ManualLegRequest(BaseModel):
    symbol: str = "NIFTY"
    strike: float = Field(..., gt=0, description="Strike Price")
    option_type: str = Field(..., pattern="^(CE|PE)$", description="CE or PE")
    side: str = Field(..., pattern="^(BUY|SELL)$", description="BUY or SELL")
    expiry_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", description="YYYY-MM-DD")
    quantity: int = Field(..., gt=0, le=1800, description="Qty per leg (Max 1800 freeze limit)")

class ManualTradeRequest(BaseModel):
    strategy_name: str = "MANUAL"
    legs: List[ManualLegRequest]
    capital_bucket: CapitalBucket = CapitalBucket.INTRADAY # Default to Intraday bucket
    tag: str = "Discretionary"

# ==================== DATA & STATUS MODELS ====================

@dataclass
class AdvancedMetrics:
    timestamp: datetime
    spot_price: float
    vix: float
    ivp: float
    realized_vol_7d: float
    garch_vol_7d: float
    iv_rv_spread: float
    event_risk_score: float
    regime: str
    pcr: float
    max_pain: float
    term_structure_slope: float
    volatility_skew: float
    sabr_alpha: float
    sabr_beta: float
    sabr_rho: float
    sabr_nu: float

    def dict(self):
        return {k: str(v) if isinstance(v, datetime) else v for k, v in self.__dict__.items()}

@dataclass
class DashboardData:
    spot_price: float
    vix: float
    pnl: float
    capital: Dict[str, Dict[str, float]]
    trades: List[Dict]
    metrics: Dict[str, Any]

@dataclass
class EngineStatus:
    running: bool
    circuit_breaker: bool
    cycle_count: int
    total_trades: int
    daily_pnl: float
    max_equity: float
    last_metrics: Optional[AdvancedMetrics]
    dashboard_ready: bool

    def to_dict(self):
        return {
            "running": self.running,
            "circuit_breaker": self.circuit_breaker,
            "total_trades": self.total_trades,
            "daily_pnl": self.daily_pnl
        }
