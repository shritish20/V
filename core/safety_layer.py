import asyncio
import logging
from datetime import datetime
from typing import Tuple, Dict, Any
from core.models import MultiLegTrade
from core.enums import TradeStatus
from core.config import settings

logger = logging.getLogger("SafetyLayer")

class MasterSafetyLayer:
    """
    INTELLIGENCE EDITION v3.0:
    Now includes AI Pattern Matching in the approval chain.
    """
    def __init__(self, risk_manager, margin_guard, lifecycle_mgr, vrp_analyzer, ai_officer):
        self.risk_mgr = risk_manager
        self.margin_guard = margin_guard
        self.lifecycle_mgr = lifecycle_mgr
        self.vrp_analyzer = vrp_analyzer
        self.ai_officer = ai_officer  # NEW: The AI Brain
        
        # State tracking
        self.trades_today = 0
        self.last_trade_time = 0
        self.peak_equity = 0
        self.is_halted = False
        
        # Safety limits
        self.max_trades_per_day = 3
        self.min_time_between_trades = 1800 
        self.max_drawdown_pct = 0.05 
        self.max_single_trade_loss_pct = 0.01
        self.min_greek_confidence = 0.6

    async def pre_trade_gate(
        self, 
        trade: MultiLegTrade, 
        current_metrics: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        MASTER GATE - Checks Math, Margin, Greeks, AND AI History
        """
        # === GATE 1: System Halted ===
        if self.is_halted:
            return False, "🛑 SYSTEM HALTED: Trading suspended"

        # === GATE 2: Drawdown ===
        daily_pnl = getattr(self.risk_mgr, 'daily_pnl', 0.0)
        if self.peak_equity == 0:
            self.peak_equity = settings.ACCOUNT_SIZE
        self.peak_equity = max(self.peak_equity, settings.ACCOUNT_SIZE + daily_pnl)
        
        drawdown_pct = 0.0
        if self.peak_equity > 0:
            drawdown_pct = (self.peak_equity - (settings.ACCOUNT_SIZE + daily_pnl)) / self.peak_equity
            
        if drawdown_pct > self.max_drawdown_pct:
            self.is_halted = True
            logger.critical(f"🚨 DRAWDOWN HALT: {drawdown_pct*100:.1f}%")
            return False, f"Drawdown breached: {drawdown_pct*100:.1f}%"

        # === GATE 3: Limits ===
        if self.trades_today >= self.max_trades_per_day:
            return False, "Daily trade limit reached"

        # === GATE 4: Cooldown ===
        time_since_last = datetime.now().timestamp() - self.last_trade_time
        if self.last_trade_time > 0 and time_since_last < self.min_time_between_trades:
            return False, f"Cooldown: {int(self.min_time_between_trades - time_since_last)}s remaining"

        # === GATE 5: AI PATTERN RECOGNITION [NEW] ===
        # Asks the AI if this trade matches a historical failure pattern
        if self.ai_officer:
            try:
                # Construct simple context for AI
                market_ctx = {
                    "vix": current_metrics.get("vix", 0),
                    "ivp": current_metrics.get("ivp", 0),
                    "spot": current_metrics.get("spot_price", 0)
                }
                
                # Check for patterns
                approved, matches, warning = await self.ai_officer.validate_trade(trade, market_ctx)
                
                if not approved:
                    logger.warning(f"🤖 AI VETO: {warning}")
                    # We treat High Severity patterns as HARD blocks, Low as warnings
                    high_severity = any(m.get('severity') == 'HIGH' for m in matches)
                    if high_severity:
                        return False, f"AI BLOCK: {warning}"
            except Exception as e:
                logger.error(f"AI Check Failed: {e}")
                # Don't block on AI failure, proceed to math checks

        # === GATE 6: Lifecycle ===
        allowed, reason = self.lifecycle_mgr.can_enter_new_trade(trade.expiry_date, trade.expiry_type)
        if not allowed:
            return False, f"Lifecycle: {reason}"

        # === GATE 7: Greeks ===
        for leg in trade.legs:
            greeks = current_metrics.get("greeks_cache", {}).get(leg.instrument_key, {})
            confidence = greeks.get("confidence_score", 0.0)
            if confidence > 0 and confidence < self.min_greek_confidence:
                return False, f"Low Greek Confidence: {confidence}"

        # === GATE 8: Margin ===
        if self.margin_guard:
            try:
                vix = current_metrics.get("vix", 20.0)
                ok, req = await self.margin_guard.is_margin_ok(trade, vix)
                if not ok:
                    return False, f"Insufficient Margin: Need {req:,.0f}"
            except Exception:
                pass

        logger.info(f"✅ ALL GATES PASSED for {trade.id}")
        return True, "Approved"

    def post_trade_update(self, trade_executed: bool):
        if trade_executed:
            self.trades_today += 1
            self.last_trade_time = datetime.now().timestamp()

    def reset_daily_counters(self):
        self.trades_today = 0
        self.is_halted = False
