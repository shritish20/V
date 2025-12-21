import logging
from typing import List, Dict
from datetime import datetime
from core.config import settings
from core.models import MultiLegTrade, TradeStatus
from core.enums import ExitReason

logger = logging.getLogger("RiskManager")

class AdvancedRiskManager:
    def __init__(self, db_manager, alert_system):
        self.db = db_manager
        self.alerts = alert_system
        self.portfolio_delta = 0.0
        self.portfolio_vega = 0.0
        self.portfolio_gamma = 0.0
        self.daily_pnl = 0.0
        self.peak_equity = 0.0
        self.is_halted = False

    def update_portfolio_state(self, trades: List[MultiLegTrade], total_pnl: float):
        self.daily_pnl = total_pnl
        self.peak_equity = max(self.peak_equity, total_pnl)
        
        self.portfolio_delta = sum(t.trade_delta for t in trades if t.status == TradeStatus.OPEN)
        self.portfolio_vega = sum(t.trade_vega for t in trades if t.status == TradeStatus.OPEN)
        self.portfolio_gamma = sum(t.trade_gamma for t in trades if t.status == TradeStatus.OPEN)

    def check_portfolio_limits(self) -> bool:
        if self.is_halted: return True

        max_loss = settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT
        if self.daily_pnl < -max_loss:
            logger.critical(f"🛑 MAX LOSS BREACHED: {self.daily_pnl:.2f} < -{max_loss:.2f}")
            self.is_halted = True
            return True

        if abs(self.portfolio_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.warning(f"⚠️ VEGA LIMIT EXCEEDED: {self.portfolio_vega:.2f}")
            return False 

        return False

    def check_pre_trade(self, proposed_trade: MultiLegTrade) -> bool:
        if self.is_halted: return False
        
        new_vega = self.portfolio_vega + proposed_trade.trade_vega
        if abs(new_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.warning(f"🚫 Trade Rejected: Vega Limit ({new_vega:.2f})")
            return False
            
        return True
