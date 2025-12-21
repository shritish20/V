from datetime import datetime, date
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field
from core.enums import StrategyType, TradeStatus, CapitalBucket, ExpiryType, ExitReason

class GreeksSnapshot(BaseModel):
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class Position(BaseModel):
    symbol: str
    instrument_key: str
    strike: float
    option_type: str
    quantity: int
    entry_price: float
    current_price: float
    entry_time: datetime
    current_greeks: GreeksSnapshot
    expiry_type: ExpiryType
    capital_bucket: CapitalBucket

class MultiLegTrade(BaseModel):
    id: str
    legs: List[Position]
    strategy_type: StrategyType
    status: TradeStatus
    entry_time: datetime
    exit_time: Optional[datetime] = None
    net_premium_per_share: float = 0.0
    pnl: float = 0.0
    lots: int = 1
    expiry_date: str
    expiry_type: ExpiryType
    capital_bucket: CapitalBucket
    basket_order_id: Optional[str] = None
    exit_reason: Optional[ExitReason] = None

    @property
    def trade_vega(self) -> float:
        return sum(l.current_greeks.vega * l.quantity for l in self.legs)

    @property
    def trade_delta(self) -> float:
        return sum(l.current_greeks.delta * l.quantity for l in self.legs)
    
    @property
    def trade_gamma(self) -> float:
        return sum(l.current_greeks.gamma * l.quantity for l in self.legs)

    def total_unrealized_pnl(self) -> float:
        return sum((l.current_price - l.entry_price) * l.quantity for l in self.legs)

class AdvancedMetrics(BaseModel):
    timestamp: datetime
    spot_price: float
    vix: float
    
    # Volatility Metrics
    ivp: float       # Frequency (Percentile)
    iv_rank: float   # Relative Range (Rank)
    
    realized_vol_7d: float
    realized_vol_28d: float = 0.0
    garch_vol_7d: float
    egarch_vol_1d: float = 0.0
    
    # Term Structure & Skew
    atm_iv: float
    monthly_iv: float = 0.0
    vrp_score: float
    vrp_zscore: float = 0.0
    iv_rv_spread: float
    term_structure_slope: float
    volatility_skew: float
    
    # Execution Context
    straddle_price: float
    straddle_price_monthly: float = 0.0
    structure_confidence: float
    
    # Regime
    regime: str
    event_risk_score: float
    top_event: str
    trend_status: str
    
    # Expiry Data
    days_to_expiry: float
    expiry_date: str
    pcr: float
    max_pain: float
    
    # SABR
    sabr_alpha: float = 0.0
    sabr_beta: float = 0.0
    sabr_rho: float = 0.0
    sabr_nu: float = 0.0
    
    # Extras
    efficiency_table: List[Dict] = []

class Order(BaseModel):
    instrument_key: str
    quantity: int
    transaction_type: str
    order_type: str
    product: str
    price: float
    trigger_price: float
    validity: str
    is_amo: bool = False

# --- Manual Trade Models ---
class ManualLegRequest(BaseModel):
    symbol: str = "NIFTY"
    strike: float
    option_type: str
    expiry_date: str
    side: str
    quantity: int

class ManualTradeRequest(BaseModel):
    legs: List[ManualLegRequest]
    capital_bucket: CapitalBucket
    note: str = ""

class EngineStatus(BaseModel):
    running: bool
    circuit_breaker: bool
    cycle_count: int
    total_trades: int
    daily_pnl: float
    max_equity: float
    last_metrics: Optional[AdvancedMetrics]
    dashboard_ready: bool
