from core.config import settings
from core.enums import TradeStatus
from utils.logger import setup_logger

logger = setup_logger("RiskMgr")

class AdvancedRiskManager:
    def __init__(self, db, alerts):
        self.db = db
        self.alerts = alerts
        self.portfolio_vega = 0.0
        self.portfolio_delta = 0.0
        self.daily_pnl = 0.0

    def check_pre_trade(self, trade):
        trade_vega = sum(
            (l.current_greeks.vega or 0.0) * l.quantity for l in trade.legs
        )
        if abs(self.portfolio_vega + trade_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.warning(
                f"🚫 Trade Rejected: Vega Limit. "
                f"New: {self.portfolio_vega + trade_vega:.0f}"
            )
            return False

        if self.daily_pnl < -(settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT):
            logger.warning("🚫 Trade Rejected: Daily Loss Limit Reached")
            return False

        return True

    def check_correlation_risk(self, new_trade, active_trades: list) -> bool:
        current_portfolio_delta = sum(
            getattr(t, "trade_delta", 0.0) for t in active_trades
        )
        new_trade_delta = sum(
            (l.current_greeks.delta or 0.0) * l.quantity for l in new_trade.legs
        )

        if abs(current_portfolio_delta + new_trade_delta) > settings.MAX_PORTFOLIO_DELTA:
            logger.warning(
                f"🚫 REJECTED: Delta Correlation. "
                f"New Total: {current_portfolio_delta + new_trade_delta:.0f}"
            )
            return False
        return True

    def update_portfolio_state(self, trades, pnl):
        """Correct ENUM comparison — prevents portfolio vega/delta from becoming zero."""
        self.daily_pnl = pnl

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

    def check_portfolio_limits(self) -> bool:
        if self.daily_pnl < -(settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT):
            logger.critical(f"Daily Loss Breach: {self.daily_pnl:.0f}")
            return True

        if abs(self.portfolio_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.critical(f"Vega Limit Breach: {self.portfolio_vega:.0f}")
            return True

        return False
