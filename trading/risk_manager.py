from core.config import settings
from core.enums import TradeStatus
from utils.logger import setup_logger

logger = setup_logger("RiskMgr")

class AdvancedRiskManager:
    def __init__(self, db, alerts):
        self.db = db
        self.alerts = alerts
        
        # Portfolio Greeks tracking
        self.portfolio_vega = 0.0
        self.portfolio_delta = 0.0
        self.portfolio_gamma = 0.0  # ADDED
        self.portfolio_theta = 0.0  # ADDED
        
        # PnL tracking
        self.daily_pnl = 0.0
        self.max_drawdown = 0.0
        self.peak_equity = settings.ACCOUNT_SIZE

    def check_pre_trade(self, trade) -> bool:
        """
        Pre-trade risk checks before order placement
        Returns True if trade is safe to execute
        """
        # 1. Check Vega exposure
        trade_vega = sum(
            (l.current_greeks.vega or 0.0) * l.quantity for l in trade.legs
        )
        
        if abs(self.portfolio_vega + trade_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.warning(
                f"🚫 Trade Rejected: Vega Limit. "
                f"Current: {self.portfolio_vega:.0f} "
                f"New: {self.portfolio_vega + trade_vega:.0f} "
                f"Limit: {settings.MAX_PORTFOLIO_VEGA:.0f}"
            )
            return False

        # 2. Check Delta exposure
        trade_delta = sum(
            (l.current_greeks.delta or 0.0) * l.quantity for l in trade.legs
        )
        
        if abs(self.portfolio_delta + trade_delta) > settings.MAX_PORTFOLIO_DELTA:
            logger.warning(
                f"🚫 Trade Rejected: Delta Limit. "
                f"Current: {self.portfolio_delta:.0f} "
                f"New: {self.portfolio_delta + trade_delta:.0f} "
                f"Limit: {settings.MAX_PORTFOLIO_DELTA:.0f}"
            )
            return False

        # 3. Check Gamma exposure (ADDED)
        trade_gamma = sum(
            (l.current_greeks.gamma or 0.0) * l.quantity for l in trade.legs
        )
        
        if abs(self.portfolio_gamma + trade_gamma) > settings.MAX_PORTFOLIO_GAMMA:
            logger.warning(
                f"🚫 Trade Rejected: Gamma Limit. "
                f"Current: {self.portfolio_gamma:.0f} "
                f"New: {self.portfolio_gamma + trade_gamma:.0f} "
                f"Limit: {settings.MAX_PORTFOLIO_GAMMA:.0f}"
            )
            return False

        # 4. Check Theta exposure (ADDED)
        trade_theta = sum(
            (l.current_greeks.theta or 0.0) * l.quantity for l in trade.legs
        )
        
        # Theta is typically negative for sellers, so we check absolute value
        if abs(self.portfolio_theta + trade_theta) > abs(settings.MAX_PORTFOLIO_THETA):
            logger.warning(
                f"🚫 Trade Rejected: Theta Limit. "
                f"Current: {self.portfolio_theta:.0f} "
                f"New: {self.portfolio_theta + trade_theta:.0f} "
                f"Limit: {settings.MAX_PORTFOLIO_THETA:.0f}"
            )
            return False

        # 5. Check Daily Loss Limit
        if self.daily_pnl < -(settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT):
            logger.warning(
                f"🚫 Trade Rejected: Daily Loss Limit Reached. "
                f"PnL: {self.daily_pnl:.0f}"
            )
            return False

        # 6. Check Maximum Drawdown
        current_equity = settings.ACCOUNT_SIZE + self.daily_pnl
        if current_equity > self.peak_equity:
            self.peak_equity = current_equity
        
        drawdown_pct = (self.peak_equity - current_equity) / self.peak_equity
        if drawdown_pct > 0.10:  # 10% max drawdown
            logger.warning(
                f"🚫 Trade Rejected: Max Drawdown Exceeded. "
                f"Drawdown: {drawdown_pct*100:.1f}%"
            )
            return False

        return True

    def check_correlation_risk(self, new_trade, active_trades: list) -> bool:
        """
        Check if new trade has excessive correlation with existing positions
        """
        current_portfolio_delta = sum(
            getattr(t, "trade_delta", 0.0) for t in active_trades
            if t.status == TradeStatus.OPEN
        )
        
        new_trade_delta = sum(
            (l.current_greeks.delta or 0.0) * l.quantity for l in new_trade.legs
        )

        if abs(current_portfolio_delta + new_trade_delta) > settings.MAX_PORTFOLIO_DELTA:
            logger.warning(
                f"🚫 REJECTED: Delta Correlation. "
                f"Portfolio Delta: {current_portfolio_delta:.0f} "
                f"New Trade Delta: {new_trade_delta:.0f} "
                f"Total Would Be: {current_portfolio_delta + new_trade_delta:.0f}"
            )
            return False

        return True

    def update_portfolio_state(self, trades, pnl):
        """
        FIXED: Update all portfolio risk metrics including Gamma and Theta
        """
        self.daily_pnl = pnl
        
        # Calculate portfolio Greeks from active trades only
        self.portfolio_vega = sum(
            getattr(t, "trade_vega", 0.0)
            for t in trades
            if t.status in {TradeStatus.OPEN, TradeStatus.EXTERNAL}
        )
        
        self.portfolio_delta = sum(
            getattr(t, "trade_delta", 0.0)
            for t in trades
            if t.status in {TradeStatus.OPEN, TradeStatus.EXTERNAL}
        )
        
        # ADDED: Gamma tracking
        self.portfolio_gamma = sum(
            getattr(t, "trade_gamma", 0.0)
            for t in trades
            if t.status in {TradeStatus.OPEN, TradeStatus.EXTERNAL}
        )
        
        # ADDED: Theta tracking
        self.portfolio_theta = sum(
            getattr(t, "trade_theta", 0.0)
            for t in trades
            if t.status in {TradeStatus.OPEN, TradeStatus.EXTERNAL}
        )

    def check_portfolio_limits(self) -> bool:
        """
        ENHANCED: Check all portfolio limits including Gamma and Theta
        Returns True if any limit is breached
        """
        breached = False
        
        # Check Daily Loss Limit
        daily_loss_limit = settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT
        if self.daily_pnl < -daily_loss_limit:
            logger.critical(
                f"💥 Daily Loss Breach: {self.daily_pnl:.0f} "
                f"(Limit: {-daily_loss_limit:.0f})"
            )
            breached = True

        # Check Vega Limit
        if abs(self.portfolio_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.critical(
                f"💥 Vega Limit Breach: {self.portfolio_vega:.0f} "
                f"(Limit: {settings.MAX_PORTFOLIO_VEGA:.0f})"
            )
            breached = True

        # Check Delta Limit
        if abs(self.portfolio_delta) > settings.MAX_PORTFOLIO_DELTA:
            logger.critical(
                f"💥 Delta Limit Breach: {self.portfolio_delta:.0f} "
                f"(Limit: {settings.MAX_PORTFOLIO_DELTA:.0f})"
            )
            breached = True

        # ADDED: Check Gamma Limit
        if abs(self.portfolio_gamma) > settings.MAX_PORTFOLIO_GAMMA:
            logger.critical(
                f"💥 Gamma Limit Breach: {self.portfolio_gamma:.0f} "
                f"(Limit: {settings.MAX_PORTFOLIO_GAMMA:.0f})"
            )
            breached = True

        # ADDED: Check Theta Limit
        if abs(self.portfolio_theta) > abs(settings.MAX_PORTFOLIO_THETA):
            logger.critical(
                f"💥 Theta Limit Breach: {self.portfolio_theta:.0f} "
                f"(Limit: {settings.MAX_PORTFOLIO_THETA:.0f})"
            )
            breached = True

        return breached

    def get_risk_metrics(self) -> dict:
        """Get current risk metrics snapshot"""
        return {
            "daily_pnl": self.daily_pnl,
            "portfolio_delta": self.portfolio_delta,
            "portfolio_gamma": self.portfolio_gamma,
            "portfolio_theta": self.portfolio_theta,
            "portfolio_vega": self.portfolio_vega,
            "peak_equity": self.peak_equity,
            "current_drawdown_pct": (self.peak_equity - (settings.ACCOUNT_SIZE + self.daily_pnl)) / self.peak_equity * 100,
        }
