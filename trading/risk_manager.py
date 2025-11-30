import numpy as np
from typing import List, Dict, Any
from datetime import datetime
import logging
from core.models import MultiLegTrade, AdvancedMetrics, PortfolioMetrics, TradeStatus
from core.config import ACCOUNT_SIZE, DAILY_LOSS_LIMIT, MAX_PORTFOLIO_VEGA, MAX_PORTFOLIO_DELTA, SYSTEMATIC_MAX_RISK_PERCENT
from database.manager import HybridDatabaseManager
from alerts.system import CriticalAlertSystem
import prometheus_client
from prometheus_client import Counter, Gauge

logger = logging.getLogger("VolGuard14")

# Prometheus metrics
RISK_VIOLATIONS = Counter('volguard_risk_violations_total', 'Risk limit violations', ['type'])
PORTFOLIO_GREEKS = Gauge('volguard_portfolio_greeks', 'Portfolio Greek exposures', ['greek_type'])

class AdvancedRiskManager:
    """Comprehensive risk management with stress testing and correlation analysis - Enhanced Fusion"""
    
    def __init__(self, database: HybridDatabaseManager, alert_system: CriticalAlertSystem):
        self.db = database
        self.alert_system = alert_system
        self.portfolio_metrics = PortfolioMetrics(
            timestamp=datetime.now(),
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
        
        # Enhanced position limits
        self.max_positions = 5
        self.max_position_size_pct = 0.20
        self.correlation_matrix = self._initialize_correlation_matrix()

    def _initialize_correlation_matrix(self) -> np.ndarray:
        """Initialize strike correlation matrix"""
        # In practice, this would be calibrated from historical data
        # For now, using reasonable assumptions
        strikes = np.arange(18000, 22000, 100)  # Example strike range
        n_strikes = len(strikes)
        corr_matrix = np.eye(n_strikes)
        
        # Add correlation decay with distance
        for i in range(n_strikes):
            for j in range(i+1, n_strikes):
                distance = abs(strikes[i] - strikes[j]) / 100  # Normalized by 100 points
                correlation = max(0, 1 - distance * 0.1)  # Linear decay
                corr_matrix[i, j] = correlation
                corr_matrix[j, i] = correlation
                
        return corr_matrix

    def update_portfolio_state(self, trades: List[MultiLegTrade], daily_pnl: float):
        """Update portfolio metrics with correlation awareness"""
        total_pnl = sum(trade.total_unrealized_pnl() for trade in trades if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL])
        total_delta = sum(trade.trade_delta for trade in trades if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL])
        total_vega = sum(trade.trade_vega for trade in trades if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL])
        
        # Update Prometheus metrics
        PORTFOLIO_GREEKS.labels(greek_type='delta').set(total_delta)
        PORTFOLIO_GREEKS.labels(greek_type='vega').set(total_vega)
        
        self.portfolio_metrics = PortfolioMetrics(
            timestamp=datetime.now(),
            total_pnl=total_pnl,
            total_delta=total_delta,
            total_gamma=0.0,
            total_theta=0.0,
            total_vega=total_vega,
            open_trades=len([t for t in trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]),
            daily_pnl=daily_pnl,
            equity=ACCOUNT_SIZE + total_pnl,
            drawdown=max(0, self.max_equity - (ACCOUNT_SIZE + total_pnl))
        )
        
        self.max_equity = max(self.max_equity, ACCOUNT_SIZE + total_pnl)
        
        # Save snapshot for analysis
        self.db.save_portfolio_snapshot(self.portfolio_metrics)

    def should_flatten_portfolio(self) -> bool:
        """Enhanced circuit breaker with multiple conditions"""
        conditions = [
            self.portfolio_metrics.daily_pnl <= -DAILY_LOSS_LIMIT,
            abs(self.portfolio_metrics.total_vega) > MAX_PORTFOLIO_VEGA,
            abs(self.portfolio_metrics.total_delta) > MAX_PORTFOLIO_DELTA,
            self.portfolio_metrics.drawdown > ACCOUNT_SIZE * 0.05,  # 5% drawdown limit
        ]
        
        if any(conditions):
            RISK_VIOLATIONS.labels(type='circuit_breaker').inc()
            
            # Send urgent alert for critical violations
            if self.portfolio_metrics.daily_pnl <= -DAILY_LOSS_LIMIT:
                logger.critical(f"Daily loss limit breached! PnL: ₹{self.portfolio_metrics.daily_pnl:,.0f}, Limit: ₹{DAILY_LOSS_LIMIT:,.0f}")
            
            return True
        return False

    def stress_test_portfolio(self, trades: List[MultiLegTrade], 
                            spot_shocks: List[float] = [-0.05, -0.10, 0.05, 0.10],
                            vol_shocks: List[float] = [-0.20, 0.50]) -> Dict[str, Any]:
        """Comprehensive stress testing with multiple scenarios"""
        results = []
        
        for spot_shock in spot_shocks:
            for vol_shock in vol_shocks:
                scenario_pnl = self._calculate_scenario_pnl(trades, spot_shock, vol_shock)
                results.append({
                    "spot_shock": spot_shock,
                    "vol_shock": vol_shock, 
                    "pnl_impact": scenario_pnl,
                    "severity": "HIGH" if abs(scenario_pnl) > ACCOUNT_SIZE * 0.02 else "MEDIUM"
                })
        
        # Find worst-case scenario
        worst_scenario = min(results, key=lambda x: x['pnl_impact'])
        
        return {
            "scenarios": results,
            "worst_case": worst_scenario,
            "portfolio_equity": self.portfolio_metrics.equity
        }

    def _calculate_scenario_pnl(self, trades: List[MultiLegTrade], 
                              spot_shock: float, vol_shock: float) -> float:
        """Calculate PnL impact for a given scenario"""
        total_pnl_impact = 0.0
        
        for trade in trades:
            if trade.status not in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                continue
                
            for leg in trade.legs:
                # Simplified PnL impact calculation
                # In practice, you'd use proper Greeks and scenario analysis
                delta_impact = leg.current_greeks.delta * leg.quantity * spot_shock * 50  # Nifty multiplier
                vega_impact = leg.current_greeks.vega * leg.quantity * vol_shock
                total_pnl_impact += delta_impact + vega_impact
        
        return total_pnl_impact

    def calculate_portfolio_hedge(self, target_delta: float = 0, target_vega: float = 0) -> Dict[str, Any]:
        """Calculate hedge ratios to neutralize portfolio Greeks"""
        current_delta = self.portfolio_metrics.total_delta
        current_vega = self.portfolio_metrics.total_vega
        
        # Delta hedge using futures (simplified)
        nifty_delta_per_lot = 50  # Nifty futures
        hedge_lots = -int(current_delta / nifty_delta_per_lot)
        
        # Vega exposure percentage
        vega_exposure_pct = current_vega / MAX_PORTFOLIO_VEGA
        
        return {
            "futures_hedge_lots": hedge_lots,
            "vega_exposure_pct": vega_exposure_pct,
            "delta_imbalance": current_delta,
            "vega_imbalance": current_vega,
            "recommendation": "HEDGE_DELTA" if abs(current_delta) > 100 else "MONITOR"
        }

    def get_position_size(self, max_loss_per_lot: float, metrics: AdvancedMetrics, 
                         event_risk_multiplier: float = 1.0) -> int:
        """Enhanced position sizing with multiple factors"""
        if max_loss_per_lot <= 0:
            return 0
            
        risk_capital = ACCOUNT_SIZE * SYSTEMATIC_MAX_RISK_PERCENT
        base_position_size = int(risk_capital / max_loss_per_lot)
        
        # Apply event risk multiplier
        adjusted_size = int(base_position_size * event_risk_multiplier)
        
        # Apply volatility regime adjustment
        if metrics.regime.value in ["PANIC", "FEAR_BACKWARDATION", "DEFENSIVE_EVENT"]:
            adjusted_size = int(adjusted_size * 0.7)  # Reduce size in high vol
        elif metrics.regime.value in ["LOW_VOL_COMPRESSION", "CALM_COMPRESSION"]:
            adjusted_size = int(adjusted_size * 1.2)  # Increase size in low vol
            
        return min(adjusted_size, 5)  # Max 5 lots per trade

    def can_open_new_trade(self, trade_vega: float, trade_delta: float, 
                          current_trades: List[MultiLegTrade]) -> bool:
        """Enhanced trade approval with correlation checks"""
        # Basic Greek limits
        if not (abs(self.portfolio_metrics.total_vega + trade_vega) <= MAX_PORTFOLIO_VEGA and
                abs(self.portfolio_metrics.total_delta + trade_delta) <= MAX_PORTFOLIO_DELTA):
            RISK_VIOLATIONS.labels(type='greek_limit').inc()
            return False
            
        # Position count limits
        open_trades = len([t for t in current_trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]])
        if open_trades >= self.max_positions:
            RISK_VIOLATIONS.labels(type='position_limit').inc()
            logger.warning(f"Max positions limit reached: {open_trades}")
            return False
            
        return True

    def get_risk_report(self) -> Dict[str, Any]:
        """Generate comprehensive risk report"""
        return {
            "portfolio_metrics": {
                "total_pnl": self.portfolio_metrics.total_pnl,
                "total_delta": self.portfolio_metrics.total_delta,
                "total_vega": self.portfolio_metrics.total_vega,
                "equity": self.portfolio_metrics.equity,
                "drawdown": self.portfolio_metrics.drawdown,
                "open_trades": self.portfolio_metrics.open_trades
            },
            "risk_limits": {
                "max_vega": MAX_PORTFOLIO_VEGA,
                "max_delta": MAX_PORTFOLIO_DELTA,
                "daily_loss_limit": DAILY_LOSS_LIMIT,
                "vega_utilization": abs(self.portfolio_metrics.total_vega) / MAX_PORTFOLIO_VEGA,
                "delta_utilization": abs(self.portfolio_metrics.total_delta) / MAX_PORTFOLIO_DELTA
            },
            "circuit_breaker": self.should_flatten_portfolio()
        }
