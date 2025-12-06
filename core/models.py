from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from decimal import Decimal
from core.config import settings, IST
from core.enums import *

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
            'delta': self.delta,
            'gamma': self.gamma,
            'theta': self.theta,
            'vega': self.vega,
            'iv': self.iv,
            'pop': self.pop,
            'charm': self.charm,
            'vanna': self.vanna,
            'timestamp': self.timestamp.isoformat()
        }

class Position(BaseModel):
    symbol: str
    instrument_key: str
    strike: float
    option_type: str  # CE or PE
    quantity: int
    entry_price: float = Field(gt=0.0)
    entry_time: datetime
    current_price: float = Field(gt=0.0)
    current_greeks: GreeksSnapshot
    transaction_costs: float = Field(ge=0.0, default=0.0)
    expiry_type: ExpiryType = ExpiryType.WEEKLY
    capital_bucket: CapitalBucket = CapitalBucket.WEEKLY
    tags: List[str] = Field(default_factory=list)

    @validator('option_type')
    def validate_option_type(cls, v):
        if v not in ['CE', 'PE']:
            raise ValueError('option_type must be either "CE" or "PE"')
        return v

    def unrealized_pnl(self) -> float:
        price_change = self.current_price - self.entry_price
        return (price_change * self.quantity) - self.transaction_costs

    def position_value(self) -> float:
        return abs(self.current_price * self.quantity)

    def get_moneyness(self, spot: float) -> float:
        return ((self.strike - spot) / spot) * 100

    def update_price(self, new_price: float):
        self.current_price = new_price

    def update_greeks(self, new_greeks: GreeksSnapshot):
        self.current_greeks = new_greeks

class MultiLegTrade(BaseModel):
    legs: List[Position]
    strategy_type: StrategyType
    net_premium_per_share: float
    entry_time: datetime
    lots: int = Field(gt=0, default=1)
    status: TradeStatus = TradeStatus.OPEN
    expiry_date: str
    expiry_type: ExpiryType
    capital_bucket: CapitalBucket

    max_loss_per_lot: float = Field(ge=0.0, default=0.0)
    max_profit_per_lot: float = Field(ge=0.0, default=0.0)
    breakeven_lower: float = 0.0
    breakeven_upper: float = 0.0
    transaction_costs: float = Field(ge=0.0, default=0.0)

    basket_order_id: Optional[str] = None
    gtt_order_ids: List[str] = Field(default_factory=list)

    trade_vega: float = 0.0
    trade_delta: float = 0.0
    trade_theta: float = 0.0
    trade_gamma: float = 0.0

    id: Optional[int] = None
    exit_reason: Optional[ExitReason] = None
    exit_time: Optional[datetime] = None
    tags: List[str] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self.calculate_metrics()

    def calculate_metrics(self):
        self.calculate_max_loss()
        self.calculate_max_profit()
        self.calculate_breakevens()
        self.calculate_trade_greeks()
        self.calculate_transaction_costs()

    def calculate_max_loss(self):
        if self.strategy_type in [StrategyType.IRON_CONDOR]:
            strikes = sorted({leg.strike for leg in self.legs})
            if len(strikes) >= 2:
                spread_width = strikes[-1] - strikes[0]
                self.max_loss_per_lot = max(0.0, (spread_width / settings.LOT_SIZE) - self.net_premium_per_share)
        elif self.strategy_type == StrategyType.ATM_STRANGLE:
            self.max_loss_per_lot = float('inf')
        else:
            self.max_loss_per_lot = self.net_premium_per_share

    def calculate_max_profit(self):
        if self.strategy_type in [StrategyType.ATM_STRADDLE, StrategyType.ATM_STRANGLE]:
            self.max_profit_per_lot = float('inf')
        else:
            self.max_profit_per_lot = self.net_premium_per_share

    def calculate_breakevens(self):
        if self.strategy_type == StrategyType.ATM_STRADDLE:
            self.breakeven_lower = min(leg.strike for leg in self.legs) - self.net_premium_per_share
            self.breakeven_upper = max(leg.strike for leg in self.legs) + self.net_premium_per_share
        elif self.strategy_type == StrategyType.ATM_STRANGLE:
            call_strike = max(leg.strike for leg in self.legs if leg.option_type == 'CE')
            put_strike = min(leg.strike for leg in self.legs if leg.option_type == 'PE')
            self.breakeven_upper = call_strike + self.net_premium_per_share
            self.breakeven_lower = put_strike - self.net_premium_per_share
        else:
            self.breakeven_lower = 0.0
            self.breakeven_upper = 0.0

    def calculate_trade_greeks(self):
        self.trade_delta = sum(leg.current_greeks.delta * leg.quantity for leg in self.legs)
        self.trade_gamma = sum(leg.current_greeks.gamma * leg.quantity for leg in self.legs)
        self.trade_theta = sum(leg.current_greeks.theta * leg.quantity for leg in self.legs)
        self.trade_vega = sum(leg.current_greeks.vega * leg.quantity for leg in self.legs)

    def calculate_transaction_costs(self):
        self.transaction_costs = len(self.legs) * settings.BROKERAGE_PER_ORDER * (1 + settings.GST_RATE)

    def total_unrealized_pnl(self) -> float:
        return sum(leg.unrealized_pnl() for leg in self.legs) - self.transaction_costs

    def update_prices(self, spot: float, quotes: Dict[str, float]):
        for leg in self.legs:
            leg.update_price(quotes.get(leg.symbol, leg.current_price))
        self.calculate_trade_greeks()

@dataclass
class PortfolioMetrics:
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    unrealized_pnl: float = 0.0
    margin_used: float = 0.0

@dataclass
class AdvancedMetrics:
    timestamp: datetime
    spot_price: float
    vix: float
    ivp: float
    realized_vol_7d: float
    garch_vol_7d: float
    iv_rv_spread: float
    pcr: float
    max_pain: float
    event_risk_score: float
    regime: MarketRegime
    term_structure_slope: float
    volatility_skew: float
    sabr_alpha: float
    sabr_beta: float
    sabr_rho: float
    sabr_nu: float

@dataclass
class DashboardData:
    timestamp: datetime
    spot_price: float
    vix: float
    ivp: float
    atm_strike: float
    straddle_price: float
    expected_move_pct: float
    breakeven_lower: float
    breakeven_upper: float
    atm_iv: float
    realized_vol_7d: float
    garch_vol_7d: float
    iv_rv_spread: float
    total_theta: float
    total_vega: float
    delta: float
    gamma: float
    pop: float
    days_to_expiry: int
    pcr: float
    max_pain: float
    regime: str
    event_risk: float
    full_chain: List[Dict]
    volatility_surface: List[Dict]
    term_structure: List[Dict]
    iv_skews: Dict[str, float]
    capital_allocation: Dict[str, float]
    capital_used: Dict[str, float]
    capital_available: Dict[str, float]
    recommended_strategies: List[str]

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

