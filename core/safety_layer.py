import asyncio
import logging
from datetime import datetime
from typing import Tuple
from core.models import MultiLegTrade
from core.enums import TradeStatus, ExitReason, ExpiryType
from core.config import settings, IST

logger = logging.getLogger("SafetyLayer")

class MasterSafetyLayer:
    """
    ENHANCED v2.0: Added single-trade loss limit (1% per trade)
    """
    
    def __init__(self, risk_manager, margin_guard, lifecycle_mgr, vrp_analyzer):
        self.risk_mgr = risk_manager
        self.margin_guard = margin_guard
        self.lifecycle_mgr = lifecycle_mgr
        self.vrp_analyzer = vrp_analyzer
        
        self.trades_today = 0
        self.last_trade_time = 0
        self.peak_equity = 0
        self.is_halted = False
        
        # Safety limits
        self.max_trades_per_day = 3
        self.min_time_between_trades = 1800
        self.max_drawdown_pct = 0.05
        self.max_single_trade_loss_pct = 0.01 # 1% per trade
        self.min_greek_confidence = 0.6
        
    async def pre_trade_gate(self, trade: MultiLegTrade, current_metrics: dict) -> Tuple[bool, str]:
        # 1. Halt Check
        if self.is_halted: return False, "🛑 SYSTEM HALTED"
        
        # 2. Drawdown Check
        daily_pnl = self.risk_mgr.daily_pnl
        if self.peak_equity == 0: self.peak_equity = settings.ACCOUNT_SIZE
        self.peak_equity = max(self.peak_equity, settings.ACCOUNT_SIZE + daily_pnl)
        
        drawdown_pct = 0.0
        if self.peak_equity > 0:
            drawdown_pct = (self.peak_equity - (settings.ACCOUNT_SIZE + daily_pnl)) / self.peak_equity
        
        if drawdown_pct > self.max_drawdown_pct:
            self.is_halted = True
            logger.critical(f"🚨 DRAWDOWN HALT: {drawdown_pct*100:.1f}%")
            return False, f"Drawdown limit breached: {drawdown_pct*100:.1f}%"
        
        # 3. Frequency & Time Checks
        if self.trades_today >= self.max_trades_per_day: return False, "Daily limit reached"
        if (datetime.now().timestamp() - self.last_trade_time) < self.min_time_between_trades: return False, "Cooldown active"
        
        # 4. Lifecycle
        allowed, reason = self.lifecycle_mgr.can_enter_new_trade(trade.expiry_date, trade.expiry_type)
        if not allowed: return False, reason
        
        # 5. Greeks
        for leg in trade.legs:
            greeks = current_metrics.get("greeks_cache", {}).get(leg.instrument_key, {})
            if greeks.get("confidence_score", 0.0) < self.min_greek_confidence:
                return False, "Greek confidence too low"
        
        # 6. VRP Warning (Non-blocking)
        if self.vrp_analyzer:
            z, _, _ = self.vrp_analyzer.calculate_vrp_zscore(current_metrics.get("atm_iv", 0), current_metrics.get("vix", 0))
            if z < -1.0 and trade.strategy_type.value in ["SHORT_STRANGLE", "IRON_CONDOR"]:
                logger.warning(f"⚠️ VRP WARNING: Z-Score {z:.2f}")

        # 7. Risk & Margin
        if not self.risk_mgr.check_pre_trade(trade): return False, "Risk Manager Rejected"
        
        if self.margin_guard:
            ok, req = await self.margin_guard.is_margin_ok(trade, current_metrics.get("vix", 15))
            if not ok: return False, f"Insufficient Margin: {req}"
            
        logger.info(f"✅ SAFETY GATES PASSED: {trade.strategy_type.value}")
        return True, "Approved"
    
    async def monitor_position_losses(self, trade: MultiLegTrade) -> bool:
        """
        NEW: Returns True if trade should be closed due to max loss.
        """
        if trade.status != TradeStatus.OPEN: return False
        
        pnl = trade.total_unrealized_pnl()
        loss_pct = pnl / settings.ACCOUNT_SIZE
        
        if loss_pct < -self.max_single_trade_loss_pct:
            logger.critical(f"🚨 SINGLE TRADE LOSS: {loss_pct*100:.2f}% > {self.max_single_trade_loss_pct*100}%")
            return True
        return False
    
    def post_trade_update(self, trade_executed: bool):
        if trade_executed:
            self.trades_today += 1
            self.last_trade_time = datetime.now().timestamp()
            
    def reset_daily_counters(self):
        self.trades_today = 0
        self.is_halted = False
