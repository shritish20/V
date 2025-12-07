from core.config import settings
from core.enums import TradeStatus
from utils.logger import setup_logger

logger = setup_logger("RiskMgr")

class AdvancedRiskManager:
    def __init__(self, db, alerts):
        self.db = db
        self.alerts = alerts
        
        # Portfolio State
        self.daily_pnl = 0.0
        self.peak_equity = settings.ACCOUNT_SIZE
        
        # Greek Exposures
        self.portfolio_vega = 0.0
        self.portfolio_delta = 0.0
        self.portfolio_gamma = 0.0
        self.portfolio_theta = 0.0

    def check_pre_trade(self, trade) -> bool:
        """
        Simulate adding the trade to the portfolio. 
        Returns True if all limits are respected.
        """
        # Calculate Trade Impact
        t_vega = sum((l.current_greeks.vega or 0.0) * l.quantity for l in trade.legs)
        t_delta = sum((l.current_greeks.delta or 0.0) * l.quantity for l in trade.legs)
        t_gamma = sum((l.current_greeks.gamma or 0.0) * l.quantity for l in trade.legs)
        t_theta = sum((l.current_greeks.theta or 0.0) * l.quantity for l in trade.legs)

        # 1. Vega Check
        if abs(self.portfolio_vega + t_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.warning(f"🚫 REJECT: Vega Limit. Cur: {self.portfolio_vega:.0f} + New: {t_vega:.0f} > {settings.MAX_PORTFOLIO_VEGA}")
            return False

        # 2. Delta Check
        if abs(self.portfolio_delta + t_delta) > settings.MAX_PORTFOLIO_DELTA:
            logger.warning(f"🚫 REJECT: Delta Limit. Cur: {self.portfolio_delta:.0f} + New: {t_delta:.0f} > {settings.MAX_PORTFOLIO_DELTA}")
            return False

        # 3. Gamma Check
        if abs(self.portfolio_gamma + t_gamma) > settings.MAX_PORTFOLIO_GAMMA:
            logger.warning(f"🚫 REJECT: Gamma Limit. Cur: {self.portfolio_gamma:.0f} + New: {t_gamma:.0f} > {settings.MAX_PORTFOLIO_GAMMA}")
            return False

        # 4. Theta Check (Absolute value check)
        if abs(self.portfolio_theta + t_theta) > abs(settings.MAX_PORTFOLIO_THETA):
             logger.warning(f"🚫 REJECT: Theta Limit. Cur: {self.portfolio_theta:.0f} + New: {t_theta:.0f} > {settings.MAX_PORTFOLIO_THETA}")
             return False

        # 5. Drawdown Check
        current_equity = settings.ACCOUNT_SIZE + self.daily_pnl
        drawdown_pct = (self.peak_equity - current_equity) / self.peak_equity
        if drawdown_pct > 0.10: # Hard stop at 10% drawdown
            logger.warning(f"🚫 REJECT: Max Drawdown Exceeded ({drawdown_pct*100:.1f}%)")
            return False

        return True

    def update_portfolio_state(self, trades, pnl):
        """Recalculate total portfolio risk from active trades"""
        self.daily_pnl = pnl
        
        # Update Peak Equity for Drawdown calculation
        current_equity = settings.ACCOUNT_SIZE + pnl
        if current_equity > self.peak_equity:
            self.peak_equity = current_equity

        # Reset Greeks
        self.portfolio_vega = 0.0
        self.portfolio_delta = 0.0
        self.portfolio_gamma = 0.0
        self.portfolio_theta = 0.0

        for t in trades:
            if t.status in {TradeStatus.OPEN, TradeStatus.EXTERNAL}:
                # Summing up leg greeks
                for l in t.legs:
                    self.portfolio_vega += (l.current_greeks.vega or 0.0) * l.quantity
                    self.portfolio_delta += (l.current_greeks.delta or 0.0) * l.quantity
                    self.portfolio_gamma += (l.current_greeks.gamma or 0.0) * l.quantity
                    self.portfolio_theta += (l.current_greeks.theta or 0.0) * l.quantity

    def check_portfolio_limits(self) -> bool:
        """
        Post-update check. Returns TRUE if any limit is breached (triggering Flatten).
        """
        # Daily Loss Limit
        limit_amt = settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT
        if self.daily_pnl < -limit_amt:
            logger.critical(f"💥 DAILY LOSS LIMIT BREACHED: {self.daily_pnl:.0f} < -{limit_amt:.0f}")
            return True

        if abs(self.portfolio_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.critical("💥 VEGA LIMIT BREACHED")
            return True
            
        if abs(self.portfolio_gamma) > settings.MAX_PORTFOLIO_GAMMA:
            logger.critical("💥 GAMMA LIMIT BREACHED")
            return True

        return False
