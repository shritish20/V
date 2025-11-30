from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum
from .config import IST, LOT_SIZE, BROKERAGE_PER_ORDER, STT_RATE, GST_RATE, EXCHANGE_CHARGES, STAMP_DUTY
from .enums import TradeStatus, ExitReason, OrderStatus, OrderType, MarketRegime

@dataclass
class GreeksSnapshot:
    timestamp: datetime
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    
    def is_stale(self, max_age: float = 30.0) -> bool:
        return (datetime.now(IST) - self.timestamp).total_seconds() > max_age

class Position(BaseModel):
    """Enhanced position with transaction cost tracking"""
    symbol: str
    instrument_key: str
    strike: float
    option_type: str
    quantity: int
    entry_price: float = Field(gt=0.0)
    entry_time: datetime
    current_price: float = Field(gt=0.0)
    current_greeks: GreeksSnapshot
    transaction_costs: float = Field(ge=0.0, default=0.0)
    
    class Config:
        arbitrary_types_allowed = True
    
    def unrealized_pnl(self) -> float:
        """PnL including transaction costs"""
        price_change = self.current_price - self.entry_price
        return (price_change * self.quantity) - self.transaction_costs

class MultiLegTrade(BaseModel):
    """Enhanced trade model with state machine"""
    legs: List[Position]
    strategy_type: str
    net_premium_per_share: float
    entry_time: datetime
    lots: int = Field(gt=0)
    status: TradeStatus
    expiry_date: str
    max_loss_per_lot: float = Field(ge=0.0)
    transaction_costs: float = Field(ge=0.0, default=0.0)
    basket_order_id: Optional[str] = None
    trade_vega: float = 0.0
    trade_delta: float = 0.0
    id: Optional[int] = None
    exit_reason: Optional[ExitReason] = None
    exit_time: Optional[datetime] = None
    
    class Config:
        arbitrary_types_allowed = True

    def __post_init__(self):
        self.calculate_max_loss()
        self.calculate_trade_greeks()
        self.calculate_transaction_costs()

    def calculate_max_loss(self):
        """Calculate maximum loss per lot (simplified for standard spreads/condors)"""
        if "SPREAD" in self.strategy_type or "CONDOR" in self.strategy_type:
            strikes = sorted({leg.strike for leg in self.legs})
            if len(strikes) >= 2:
                spread_width = strikes[-1] - strikes[0]
                net_premium = self.net_premium_per_share * LOT_SIZE
                self.max_loss_per_lot = max(0.0, (spread_width / LOT_SIZE) - net_premium if net_premium > 0 else spread_width / LOT_SIZE)
                return
        self.max_loss_per_lot = float("inf")
        
    def calculate_trade_greeks(self):
        """Calculate trade-level Greeks"""
        self.trade_vega = sum(leg.current_greeks.vega * (leg.quantity / LOT_SIZE) for leg in self.legs)
        self.trade_delta = sum(leg.current_greeks.delta * leg.quantity for leg in self.legs)
        
    def calculate_transaction_costs(self):
        """Realistic transaction cost calculation (for entry and estimated exit)"""
        total_premium_value = sum(abs(leg.entry_price * leg.quantity) for leg in self.legs)
        
        brokerage = BROKERAGE_PER_ORDER * len(self.legs) * 2
        stt = total_premium_value * STT_RATE
        exchange = total_premium_value * EXCHANGE_CHARGES
        stamp = total_premium_value * STAMP_DUTY
        gst = brokerage * GST_RATE
        self.transaction_costs = brokerage + stt + exchange + stamp + gst

    def total_unrealized_pnl(self) -> float:
        return sum(leg.unrealized_pnl() for leg in self.legs) - self.transaction_costs
    
    def total_credit(self) -> float:
        return max(self.net_premium_per_share, 0) * LOT_SIZE * self.lots

    def update_greeks(self):
        """Recalculate trade-level Greeks"""
        self.trade_vega = sum(leg.current_greeks.vega * (leg.quantity / LOT_SIZE) for leg in self.legs)
        self.trade_delta = sum(leg.current_greeks.delta * leg.quantity for leg in self.legs)

class Order(BaseModel):
    """Complete order lifecycle management"""
    order_id: str
    instrument_key: str
    quantity: int
    price: float
    order_type: OrderType
    transaction_type: str  # BUY/SELL
    status: OrderStatus
    product: str = "I"
    validity: str = "DAY"
    disclosed_quantity: int = 0
    trigger_price: float = 0
    placed_time: datetime
    last_updated: datetime
    filled_quantity: int = 0
    average_price: float = 0.0
    remaining_quantity: int = 0
    retry_count: int = 0
    parent_trade_id: Optional[int] = None
    error_message: Optional[str] = None
    
    class Config:
        arbitrary_types_allowed = True
    
    def is_complete(self) -> bool:
        return self.status in [OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED]
    
    def is_active(self) -> bool:
        return self.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]
    
    def update_fill(self, filled_qty: int, avg_price: float):
        """Update order with fill information"""
        self.filled_quantity = filled_qty
        self.average_price = avg_price
        self.remaining_quantity = self.quantity - filled_qty
        self.last_updated = datetime.now()
        
        if self.remaining_quantity == 0:
            self.status = OrderStatus.FILLED
        elif filled_qty > 0:
            self.status = OrderStatus.PARTIAL_FILLED

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
    sabr_alpha: float = 0.2
    sabr_beta: float = 0.5
    sabr_rho: float = -0.2
    sabr_nu: float = 0.3
    
@dataclass
class PortfolioMetrics:
    timestamp: datetime
    total_pnl: float
    total_delta: float
    total_gamma: float
    total_theta: float
    total_vega: float
    open_trades: int
    daily_pnl: float
    equity: float
    drawdown: float
    
@dataclass
class EngineStatus:
    running: bool
    circuit_breaker: bool
    cycle_count: int
    total_trades: int
    daily_pnl: float
    max_equity: float
    last_metrics: Optional[AdvancedMetrics] = None
