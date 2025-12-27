from datetime import datetime
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field
from core.enums import StrategyType, TradeStatus, CapitalBucket, ExpiryType, ExitReason

# --- SNAPSHOTS ---
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

    def total_unrealized_pnl(self) -> float:
        return sum((l.current_price - l.entry_price) * l.quantity for l in self.legs)

# --- METRICS (PURE QUANT) ---
class AdvancedMetrics(BaseModel):
    """
    Market Data Snapshot.
    Note: 'event_risk_score' and 'top_event' are retained as placeholders
    to ensure compatibility with StrategyEngine checks, defaulting to safe values.
    """
    timestamp: datetime = Field(default_factory=datetime.now)
    spot_price: float = 0.0
    vix: float = 0.0
    
    # Volatility Metrics
    ivp: float = 0.0
    iv_rank: float = 0.0
    realized_vol_7d: float = 0.0
    realized_vol_28d: float = 0.0
    garch_vol_7d: float = 0.0
    egarch_vol_1d: float = 0.0
    
    # Term Structure & Skew
    atm_iv: float = 0.0
    monthly_iv: float = 0.0
    vrp_score: float = 0.0      
    spread_rv: float = 0.0      
    vrp_zscore: float = 0.0
    term_structure_spread: float = 0.0
    volatility_skew: float = 0.0
    
    # Execution Context
    straddle_price: float = 0.0
    straddle_price_monthly: float = 0.0
    atm_theta: float = 0.0
    atm_vega: float = 0.0
    atm_delta: float = 0.0
    atm_gamma: float = 0.0
    atm_pop: float = 0.0
    
    # Regime
    structure_confidence: float = 0.0
    regime: str = "Neutral"
    event_risk_score: float = 0.0 # Placeholder (AI Removed)
    top_event: str = "None"       # Placeholder (AI Removed)
    trend_status: str = "Flat"
    
    # Expiry Data
    days_to_expiry: float = 0.0
    expiry_date: str = "Pending"
    pcr: float = 0.0
    max_pain: float = 0.0
    
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
    tag: Optional[str] = None # Added for Sheriff compatibility

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
