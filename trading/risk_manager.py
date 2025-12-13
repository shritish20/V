from core.config import settings
from core.enums import TradeStatus
from utils.logger import setup_logger

logger = setup_logger("RiskMgr")

class AdvancedRiskManager:
    def __init__(self, db, alerts):
        self.db = db
        self.alerts = alerts
        self.daily_pnl = 0.0
        self.peak_equity = settings.ACCOUNT_SIZE
        self.portfolio_vega = 0.0
        self.portfolio_delta = 0.0
        self.portfolio_gamma = 0.0
        self.portfolio_theta = 0.0
        self.active_strikes = set()

    def check_pre_trade(self, trade) -> bool:
        # 1. HARDENING: Confidence Score Check
        min_confidence = 1.0
        for leg in trade.legs:
            score = leg.current_greeks.confidence_score
            min_confidence = min(min_confidence, score)
            
        if min_confidence < 0.5:
            logger.warning(f"🚫 REJECT: Data Integrity Risk (Confidence {min_confidence:.2f} < 0.5)")
            return False

        # 2. Position Concentration Check
        trade_notional = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        if trade_notional > settings.ACCOUNT_SIZE * 0.25:
            logger.warning("🚫 REJECT: Position Concentration > 25% of Account")
            return False

        # 3. Strike Correlation Check
        new_strikes = set(l.strike for l in trade.legs)
        if len(self.active_strikes.intersection(new_strikes)) > 0:
            logger.warning("⚠️ Warning: Adding to existing strike exposure")

        # 4. Greek Limits
        t_vega = sum((l.current_greeks.vega or 0.0) * l.quantity for l in trade.legs)
        t_delta = sum((l.current_greeks.delta or 0.0) * l.quantity for l in trade.legs)
        t_gamma = sum((l.current_greeks.gamma or 0.0) * l.quantity for l in trade.legs)

        if abs(self.portfolio_vega + t_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.warning(f"🚫 REJECT: Vega Limit ({settings.MAX_PORTFOLIO_VEGA})")
            return False
            
        if abs(self.portfolio_delta + t_delta) > settings.MAX_PORTFOLIO_DELTA:
            logger.warning(f"🚫 REJECT: Delta Limit ({settings.MAX_PORTFOLIO_DELTA})")
            return False
            
        if abs(self.portfolio_gamma + t_gamma) > settings.MAX_PORTFOLIO_GAMMA:
            logger.warning(f"🚫 REJECT: Gamma Limit ({settings.MAX_PORTFOLIO_GAMMA})")
            return False

        return True

    def update_portfolio_state(self, trades, pnl):
        self.daily_pnl = pnl
        current_equity = settings.ACCOUNT_SIZE + pnl
        if current_equity > self.peak_equity:
            self.peak_equity = current_equity

        self.portfolio_vega = 0.0
        self.portfolio_delta = 0.0
        self.portfolio_gamma = 0.0
        self.portfolio_theta = 0.0
        self.active_strikes.clear()

        for t in trades:
            if t.status in {TradeStatus.OPEN, TradeStatus.EXTERNAL}:
                for l in t.legs:
                    self.active_strikes.add(l.strike)
                    self.portfolio_vega += (l.current_greeks.vega or 0.0) * l.quantity
                    self.portfolio_delta += (l.current_greeks.delta or 0.0) * l.quantity
                    self.portfolio_gamma += (l.current_greeks.gamma or 0.0) * l.quantity
                    self.portfolio_theta += (l.current_greeks.theta or 0.0) * l.quantity

    def check_portfolio_limits(self) -> bool:
        limit_amt = settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT
        
        if self.daily_pnl < -limit_amt:
            logger.critical(f"💥 DAILY LOSS LIMIT: {self.daily_pnl:.0f}")
            return True
            
        if abs(self.portfolio_vega) > settings.MAX_PORTFOLIO_VEGA:
            logger.critical(f"💥 VEGA LIMIT BREACH: {self.portfolio_vega:.1f}")
            return True
            
        if abs(self.portfolio_gamma) > settings.MAX_PORTFOLIO_GAMMA:
            logger.critical(f"💥 GAMMA LIMIT BREACH: {self.portfolio_gamma:.1f}")
            return True
            
        return False
