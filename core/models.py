from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum
from core.config import settings, IST
from core.enums import *

# ==========================================
# ORDER MODELS
# ==========================================
class OrderStatus(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    PARTIAL = "PARTIAL"

class Order(BaseModel):
    instrument_key: str
    transaction_type: str  # "BUY" or "SELL"
    quantity: int = Field(gt=0)
    order_type: str  # "MARKET", "LIMIT", "SL", "SL-M"
    product: str  # "I", "D", "CO", "OCO", "MTF"
    price: float = 0.0
    trigger_price: float = 0.0
    validity: str = "DAY"
    is_amo: bool = False
    tag: Optional[str] = None
    
    # Response fields from Broker
    order_id: Optional[str] = None
    status: Optional[OrderStatus] = None
    average_price: Optional[float] = None
    filled_quantity: Optional[int] = None

# ==========================================
# TRADING DATA MODELS
# ==========================================
@dataclass
class GreeksSnapshot:
    timestamp: datetime
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0
    pop: float = 0.5
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
        self.trade_delta = sum((leg.current_greeks.delta or 0.0) * leg.quantity for leg in self.legs)
        self.trade_gamma = sum((leg.current_greeks.gamma or 0.0) * leg.quantity for leg in self.legs)
        self.trade_theta = sum((leg.current_greeks.theta or 0.0) * leg.quantity for leg in self.legs)
        self.trade_vega = sum((leg.current_greeks.vega or 0.0) * leg.quantity for leg in self.legs)

# ==========================================
# MANUAL REQUEST MODELS (For Terminal)
# ==========================================
class ManualLegRequest(BaseModel):
    symbol: str = "NIFTY"
    strike: float = Field(..., gt=0)
    option_type: str = Field(..., pattern="^(CE|PE)$")
    side: str = Field(..., pattern="^(BUY|SELL)$")
    expiry_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    quantity: int = Field(..., gt=0, le=1800)

class ManualTradeRequest(BaseModel):
    strategy_name: str = "MANUAL"
    legs: List[ManualLegRequest]
    capital_bucket: CapitalBucket = CapitalBucket.INTRADAY
    tag: str = "Discretionary"

# ==========================================
# QUANT & METRICS MODELS (THE UPGRADE)
# ==========================================
@dataclass
class AdvancedMetrics:
    timestamp: datetime
    spot_price: float
    vix: float
    ivp: float
    
    # --- NEW QUANT FIELDS ---
    realized_vol_7d: float
    garch_vol_7d: float
    iv_rv_spread: float      # VIX - RV (Positive = Expensive)
    volatility_skew: float   # Put IV - Call IV (Positive = Fear)
    straddle_price: float    # Market Expected Move
    # ------------------------
    
    event_risk_score: float
    regime: str
    pcr: float
    max_pain: float
    term_structure_slope: float
    
    # SABR Params
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
