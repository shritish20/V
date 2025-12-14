# File: core/models.py

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

class Order(BaseModel):
    instrument_key: str
    transaction_type: str 
    quantity: int = Field(..., gt=0)
    order_type: str       
    product: str          
    price: float = 0.0
    trigger_price: float = 0.0
    validity: str = "DAY"
    is_amo: bool = False
    tag: Optional[str] = None
    order_id: Optional[str] = None
    status: Optional[str] = None
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
    confidence_score: float = 1.0

    def is_stale(self, max_age: float = 30.0) -> bool:
        return (datetime.now(IST) - self.timestamp).total_seconds() > max_age

    def to_dict(self) -> Dict[str, float]:
        return {
            'delta': self.delta, 'gamma': self.gamma, 'theta': self.theta,
            'vega': self.vega, 'iv': self.iv, 'pop': self.pop,
            'confidence': self.confidence_score,
            'timestamp': self.timestamp.isoformat()
        }

class Position(BaseModel):
    symbol: str
    instrument_key: str
    strike: float
    option_type: str 
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
# METRICS MODELS
# ==========================================

@dataclass
class AdvancedMetrics:
    timestamp: datetime
    spot_price: float
    vix: float
    ivp: float
    
    # --- PRO METRICS ---
    realized_vol_7d: float      
    garch_vol_7d: float         
    atm_iv: float               
    vrp_score: float            
    
    term_structure_slope: float 
    volatility_skew: float      
    structure_confidence: float = 1.0 # Data Quality Flag
    
    # --- CONTEXT ---
    trend_status: str           
    event_risk_score: float     
    regime: str                 
    top_event: str = "None"     
    
    # --- EXECUTION ---
    straddle_price: float
    pcr: float
    max_pain: float
    expiry_date: str
    days_to_expiry: float       
    
    # SABR
    sabr_alpha: float = 0.0
    sabr_beta: float = 0.0
    sabr_rho: float = 0.0
    sabr_nu: float = 0.0

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
    ai_insight: Dict[str, Any]

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
