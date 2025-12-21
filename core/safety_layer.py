import asyncio
import logging
from datetime import datetime
from typing import Tuple
from core.models import MultiLegTrade
from core.config import settings, IST

logger = logging.getLogger("SafetyLayer")

class MasterSafetyLayer:
    """
    FINAL BOSS - All trades go through this
    """
    
    def __init__(self, risk_manager, margin_guard, lifecycle_mgr, vrp_analyzer):
        self.risk_mgr = risk_manager
        self.margin_guard = margin_guard
        self.lifecycle_mgr = lifecycle_mgr
        self.vrp_analyzer = vrp_analyzer
        
        # State tracking
        self.trades_today = 0
        self.last_trade_time = 0
        self.peak_equity = 0
        self.is_halted = False
        
        # Safety limits
        self.max_trades_per_day = 3
        self.min_time_between_trades = 1800  # 30 minutes
        self.max_drawdown_pct = 0.05  # 5%
        self.min_greek_confidence = 0.6
        
    async def pre_trade_gate(
        self, 
        trade: MultiLegTrade, 
        current_metrics: dict
    ) -> Tuple[bool, str]:
        """
        MASTER GATE - All checks in one place
        Returns: (approved, rejection_reason)
        """
        
        # === GATE 1: System Halted Check ===
        if self.is_halted:
            return False, "🛑 SYSTEM HALTED: Trading suspended due to drawdown"
        
        # === GATE 2: Drawdown Check ===
        daily_pnl = self.risk_mgr.daily_pnl
        if self.peak_equity == 0:
            self.peak_equity = settings.ACCOUNT_SIZE
        
        self.peak_equity = max(self.peak_equity, settings.ACCOUNT_SIZE + daily_pnl)
        
        drawdown_pct = 0.0
        if self.peak_equity > 0:
            drawdown_pct = (self.peak_equity - (settings.ACCOUNT_SIZE + daily_pnl)) / self.peak_equity
        
        if drawdown_pct > self.max_drawdown_pct:
            self.is_halted = True
            logger.critical(f"🚨 DRAWDOWN HALT: {drawdown_pct*100:.1f}% from peak")
            return False, f"Drawdown limit breached: {drawdown_pct*100:.1f}%"
        
        # === GATE 3: Daily Trade Limit ===
        if self.trades_today >= self.max_trades_per_day:
            return False, f"Daily trade limit reached: {self.trades_today}/{self.max_trades_per_day}"
        
        # === GATE 4: Entry Cooldown ===
        time_since_last = datetime.now().timestamp() - self.last_trade_time
        if self.last_trade_time > 0 and time_since_last < self.min_time_between_trades:
            remaining = int(self.min_time_between_trades - time_since_last)
            return False, f"Cooldown active: {remaining}s remaining"
        
        # === GATE 5: Lifecycle Check (Expiry Rules) ===
        allowed, lifecycle_reason = self.lifecycle_mgr.can_enter_new_trade(
            trade.expiry_date, trade.expiry_type
        )
        if not allowed:
            return False, f"Lifecycle block: {lifecycle_reason}"
        
        # === GATE 6: Greek Confidence Check ===
        for leg in trade.legs:
            greeks = current_metrics.get("greeks_cache", {}).get(leg.instrument_key, {})
            confidence = greeks.get("confidence_score", 0.0)
            
            # If no greek data yet, we might skip or block. Blocking is safer.
            if confidence > 0 and confidence < self.min_greek_confidence:
                logger.critical(f"🚫 LOW CONFIDENCE: {leg.strike} {leg.option_type} = {confidence}")
                return False, f"Greek confidence too low: {confidence} < {self.min_greek_confidence}"
        
        # === GATE 7: VRP Z-Score Filter (Warning Only) ===
        current_vix = current_metrics.get("vix", 15.0)
        current_iv = current_metrics.get("atm_iv", 15.0)
        
        if self.vrp_analyzer:
            z_score, signal, _ = self.vrp_analyzer.calculate_vrp_zscore(current_iv, current_vix)
            if z_score < -1.0 and trade.strategy_type.value in ["SHORT_STRANGLE", "IRON_CONDOR"]:
                logger.warning(f"⚠️ VRP WARNING: Z-Score = {z_score:.2f} (options cheap)")
        
        # === GATE 8: Risk Manager Approval ===
        if not self.risk_mgr.check_pre_trade(trade):
            return False, "Risk manager rejection (portfolio limits)"
        
        # === GATE 9: Margin Check (Optional) ===
        # If margin guard is implemented, use it.
        if self.margin_guard and hasattr(self.margin_guard, 'is_margin_ok'):
            try:
                margin_ok, margin_req = await self.margin_guard.is_margin_ok(trade, current_vix)
                if not margin_ok:
                    return False, f"Insufficient margin: Need ₹{margin_req:,.0f}"
            except: pass
        
        # === ALL GATES PASSED ===
        logger.info(f"✅ SAFETY GATES PASSED: {trade.strategy_type.value}")
        return True, "Approved"
    
    def post_trade_update(self, trade_executed: bool):
        """Call this after attempting trade execution"""
        if trade_executed:
            self.trades_today += 1
            self.last_trade_time = datetime.now().timestamp()
            logger.info(f"📊 Trades today: {self.trades_today}/{self.max_trades_per_day}")
    
    def reset_daily_counters(self):
        """Call this at market open (9:15 AM)"""
        self.trades_today = 0
        self.is_halted = False
        logger.info("🌅 Daily counters reset")
