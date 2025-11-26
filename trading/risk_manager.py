from threading import RLock
from typing import List
from core.models import MultiLegTrade, PortfolioMetrics, AdvancedMetrics
from core.config import ACCOUNT_SIZE, DAILY_LOSS_LIMIT, MAX_PORTFOLIO_VEGA, MAX_PORTFOLIO_DELTA, SYSTEMATIC_MAX_RISK_PERCENT, IST

class PortfolioRiskManager:
    """Comprehensive portfolio risk management"""
    
    def __init__(self):
        self.portfolio_metrics = PortfolioMetrics(
            timestamp=datetime.now(IST),
            total_pnl=0.0,
            total_delta=0.0,
            total_gamma=0.0,
            total_theta=0.0,
            total_vega=0.0,
            open_trades=0,
            daily_pnl=0.0,
            equity=ACCOUNT_SIZE,
            drawdown=0.0
        )
        self.max_equity = ACCOUNT_SIZE
        self.daily_pnl = 0.0
        self._lock = RLock()
    
    def update_portfolio_state(self, trades: List[MultiLegTrade], daily_pnl: float):
        """Update portfolio metrics"""
        with self._lock:
            total_pnl = 0.0
            total_delta = 0.0
            total_gamma = 0.0
            total_theta = 0.0
            total_vega = 0.0
            open_trades = 0
            
            for trade in trades:
                if trade.status == TradeStatus.OPEN:
                    total_pnl += trade.total_unrealized_pnl()
                    total_delta += trade.trade_delta
                    total_vega += trade.trade_vega
                    open_trades += 1
                    
                    for leg in trade.legs:
                        total_gamma += leg.current_greeks.gamma * leg.quantity
                        total_theta += leg.current_greeks.theta * leg.quantity
            
            self.daily_pnl = daily_pnl
            self.portfolio_metrics = PortfolioMetrics(
                timestamp=datetime.now(IST),
                total_pnl=total_pnl,
                total_delta=total_delta,
                total_gamma=total_gamma,
                total_theta=total_theta,
                total_vega=total_vega,
                open_trades=open_trades,
                daily_pnl=daily_pnl,
                equity=ACCOUNT_SIZE + daily_pnl + total_pnl,
                drawdown=self._calculate_drawdown(ACCOUNT_SIZE + daily_pnl + total_pnl)
            )
    
    def _calculate_drawdown(self, current_equity: float) -> float:
        """Calculate portfolio drawdown"""
        self.max_equity = max(self.max_equity, current_equity)
        if self.max_equity <= 0:
            return 0.0
        return max(0.0, (self.max_equity - current_equity) / self.max_equity)
    
    def can_open_new_trade(self, new_trade_vega: float, new_trade_delta: float) -> bool:
        """Check if new trade can be opened within risk limits"""
        with self._lock:
            # Check circuit breaker
            if self.daily_pnl <= -DAILY_LOSS_LIMIT:
                return False
            
            # Check Vega limit
            projected_vega = abs(self.portfolio_metrics.total_vega + new_trade_vega)
            if projected_vega > MAX_PORTFOLIO_VEGA:
                return False
            
            # Check Delta limit
            projected_delta = abs(self.portfolio_metrics.total_delta + new_trade_delta)
            if projected_delta > MAX_PORTFOLIO_DELTA:
                return False
            
            return True
    
    def should_flatten_portfolio(self) -> bool:
        """Check if portfolio should be flattened due to risk limits"""
        with self._lock:
            if self.daily_pnl <= -DAILY_LOSS_LIMIT:
                return True
            
            if abs(self.portfolio_metrics.total_vega) > MAX_PORTFOLIO_VEGA * 1.1:  # 10% buffer
                return True
            
            if abs(self.portfolio_metrics.total_delta) > MAX_PORTFOLIO_DELTA * 1.1:
                return True
            
            return False
    
    def get_position_size(self, max_loss_per_lot: float, metrics: AdvancedMetrics) -> int:
        """Calculate safe position size"""
        if max_loss_per_lot <= 0:
            return 0
        
        with self._lock:
            # Base risk calculation
            risk_capacity = min(
                ACCOUNT_SIZE * SYSTEMATIC_MAX_RISK_PERCENT,
                DAILY_LOSS_LIMIT - self.daily_pnl
            )
            
            # Apply event risk multiplier
            event_multiplier = 1.0
            if hasattr(metrics, 'event_risk_score'):
                if metrics.event_risk_score >= 2.5:
                    event_multiplier = 0.3
                elif metrics.event_risk_score >= 2.0:
                    event_multiplier = 0.5
                elif metrics.event_risk_score >= 1.0:
                    event_multiplier = 0.7
            
            # Apply volatility multiplier
            vol_multiplier = 1.0
            if metrics.vix > 25:
                vol_multiplier = 0.5
            elif metrics.vix > 20:
                vol_multiplier = 0.7
            elif metrics.vix < 12:
                vol_multiplier = 1.2
            
            adjusted_risk = risk_capacity * event_multiplier * vol_multiplier
            
            # Calculate lots based on risk
            risk_lots = int(adjusted_risk / max_loss_per_lot) if max_loss_per_lot > 0 else 0
            
            # Consider Vega capacity
            vega_lots = int((MAX_PORTFOLIO_VEGA - abs(self.portfolio_metrics.total_vega)) / 500.0)
            
            # Take most conservative approach
            lots = min(risk_lots, vega_lots if vega_lots > 0 else risk_lots, 5)  # Max 5 lots
            
            return max(1, lots) if lots > 0 else 0
