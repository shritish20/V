import logging
import numpy as np
from typing import Dict, List, Tuple, Optional
from core.enums import StrategyType, CapitalBucket, ExpiryType
from core.models import AdvancedMetrics
from trading.instruments_master import InstrumentMaster

logger = logging.getLogger("StrategyEngine")

class IntelligentStrategyEngine:
    """
    VolGuard 20.0 'Adaptive Prop Desk' Engine.
    Integrates: VRP Z-Score, GARCH, and Dynamic Sigma Sizing.
    """
    def __init__(self, vol_analytics, event_intel, capital_allocator, pricing_engine):
        self.vol = vol_analytics
        self.events = event_intel
        self.capital = capital_allocator
        self.pricing = pricing_engine # Use for efficiency ranking
        self.master = None

    def set_instruments_master(self, master: InstrumentMaster):
        self.master = master

    def select_strategy_with_capital(
        self, 
        metrics: AdvancedMetrics, 
        spot: float, 
        cap_status: Dict,
        ai_context: Dict = None 
    ) -> Tuple[str, List[Dict], ExpiryType, CapitalBucket]:
        
        # 1. EVENT & AI NOTIFY (Silent Observer Pattern)
        if metrics.regime == "BINARY_EVENT" or metrics.event_risk_score > 80:
            logger.warning(f"⚠️ Binary Event ({metrics.top_event}). Position: FLAT.")
            return "WAIT", [], ExpiryType.WEEKLY, CapitalBucket.INTRADAY

        ai_sentiment = ai_context.get("market_sentiment", "NEUTRAL") if ai_context else "NEUTRAL"
        ai_risk = ai_context.get("risk_level", "NEUTRAL") if ai_context else "NEUTRAL"
        
        if ai_risk in ["HIGH", "DANGER"]:
            logger.info(f"🧠 AI CIO Note: Elevated Narrative Risk ({ai_context.get('narrative')})")

        # 2. MATRIX SELECTION (Quant Driven)
        strat_name, bucket = self._determine_matrix_strategy(metrics, ai_sentiment)
        
        if strat_name == "WAIT":
            return "WAIT", [], ExpiryType.WEEKLY, CapitalBucket.INTRADAY

        # 3. CONSTRUCT LEGS (Dynamic Sigma Placement)
        legs, expiry_type = self._construct_legs(strat_name, metrics, spot)
        
        return strat_name, legs, expiry_type, bucket

    def _determine_matrix_strategy(self, m: AdvancedMetrics, ai_sentiment: str) -> Tuple[str, CapitalBucket]:
        """Regime-based logic using VRP and GARCH."""
        
        # 🚨 RULE 1: VRP SAFETY BRAKE (Claude's Recommendation)
        # If Realized Vol > Implied Vol (Negative VRP), we are selling at a loss.
        if m.vrp_zscore < -1.0:
            logger.info("🚫 Aborting: Negative VRP Z-Score (Risk exceeds Premium)")
            return "WAIT", CapitalBucket.INTRADAY

        # 🚨 RULE 2: GARCH EXPANSION CHECK
        # If GARCH predicts vol significantly higher than Market IV, go defensive.
        is_vol_expanding = m.garch_vol_7d > (m.atm_iv * 1.15)

        # REGIME: PANIC (Backwardation)
        if m.term_structure_slope > 0.05: # Near-term IV > Far-term
            return "BEAR_CALL_SPREAD", CapitalBucket.WEEKLY

        # REGIME: HIGH VOLATILITY (The Harvest)
        if m.ivp > 50:
            if is_vol_expanding:
                return "IRON_CONDOR", CapitalBucket.WEEKLY # Defined risk for expansion
            if m.vrp_zscore > 1.2 and ai_sentiment != "BEARISH":
                return "SHORT_STRANGLE", CapitalBucket.WEEKLY # High Confidence Selling
            return "IRON_CONDOR", CapitalBucket.WEEKLY

        # REGIME: HIGH SKEW (Put Panic)
        if m.volatility_skew > 8.0:
             return "RATIO_SPREAD_PUT", CapitalBucket.WEEKLY

        # REGIME: LOW VOLATILITY (The Grind)
        if m.ivp < 30:
            if ai_sentiment == "BEARISH": return "WAIT", CapitalBucket.INTRADAY
            return "JADE_LIZARD", CapitalBucket.WEEKLY

        return "IRON_CONDOR", CapitalBucket.WEEKLY

    def _construct_legs(self, strat: str, m: AdvancedMetrics, spot: float) -> Tuple[List[Dict], ExpiryType]:
        expiries = self.master.get_all_expiries("NIFTY")
        if not expiries: return [], ExpiryType.WEEKLY
        near_exp = expiries[0].strftime("%Y-%m-%d")
        
        atm = round(spot / 50) * 50

        # --- DYNAMIC SIGMA SIZING ---
        # 1-Standard Deviation daily move = Spot * (IV / 100) / sqrt(252)
        # 15.87 is sqrt(252)
        daily_sigma = spot * (max(m.atm_iv, m.vix) / 100) / 15.87
        
        # Place Short strikes at 1.5 Sigma, Wings at 2.0 Sigma
        short_dist = round((daily_sigma * 1.5) / 50) * 50
        wing_width = round((daily_sigma * 0.5) / 50) * 50
        
        # Safety floors
        short_dist = max(short_dist, 150)
        wing_width = max(wing_width, 100)

        

        legs = []
        if strat == "SHORT_STRANGLE":
            legs = [
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "CE", "strike": atm + short_dist, "side": "SELL", "expiry": near_exp}
            ]
        elif strat == "IRON_CONDOR":
            legs = [
                {"type": "CE", "strike": atm + short_dist + wing_width, "side": "BUY", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist - wing_width, "side": "BUY", "expiry": near_exp},
                {"type": "CE", "strike": atm + short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp}
            ]
        elif strat == "RATIO_SPREAD_PUT":
             legs = [
                {"type": "PE", "strike": atm, "side": "BUY", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp}
            ]
        elif strat == "JADE_LIZARD":
             legs = [
                {"type": "CE", "strike": atm + short_dist + 50, "side": "BUY", "expiry": near_exp},
                {"type": "CE", "strike": atm + short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp}
            ]
        else: # Fallback Bear Call for wait states
            legs = [{"type": "CE", "strike": atm + 200, "side": "BUY", "expiry": near_exp},
                    {"type": "CE", "strike": atm, "side": "SELL", "expiry": near_exp}]
            
        return legs, ExpiryType.WEEKLY
