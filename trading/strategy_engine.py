import logging
from typing import Dict, List, Tuple, Optional
from core.enums import StrategyType, CapitalBucket, ExpiryType
from core.models import AdvancedMetrics
from trading.instruments_master import InstrumentMaster

logger = logging.getLogger("StrategyEngine")

class IntelligentStrategyEngine:
    """
    VolGuard 19.0 'Prop Desk' Engine.
    Implements 6-Regime Matrix + AI Veto.
    """
    def __init__(self, vol_analytics, event_intel, capital_allocator, pricing_engine):
        self.vol = vol_analytics
        self.events = event_intel
        self.capital = capital_allocator
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
        
        # 1. EVENT GUARD
        # If event risk is extreme (e.g., Budget), we strictly stand aside.
        if metrics.regime == "BINARY_EVENT" or metrics.event_risk_score > 80:
            logger.warning(f"⚠️ Binary Event ({metrics.top_event}). Position: FLAT.")
            return "WAIT", [], ExpiryType.WEEKLY, CapitalBucket.INTRADAY

        # 2. AI VETO
        # The 'Narrative Filter'. If Gemini says "DANGER", we pause even if technicals are good.
        ai_risk = ai_context.get("risk_level", "NEUTRAL") if ai_context else "NEUTRAL"
        if ai_risk == "DANGER":
            logger.warning("🧠 AI Veto: DANGER Risk Level. Halting.")
            return "WAIT", [], ExpiryType.WEEKLY, CapitalBucket.INTRADAY

        ai_sentiment = ai_context.get("market_sentiment", "NEUTRAL") if ai_context else "NEUTRAL"

        # 3. MATRIX SELECTION
        strat_name, bucket = self._determine_matrix_strategy(metrics, ai_sentiment)
        
        if strat_name == "WAIT":
            return "WAIT", [], ExpiryType.WEEKLY, CapitalBucket.INTRADAY

        # 4. CONSTRUCT LEGS
        legs, expiry_type = self._construct_legs(strat_name, metrics, spot)
        
        return strat_name, legs, expiry_type, bucket

    def _determine_matrix_strategy(self, m: AdvancedMetrics, ai_sentiment: str) -> Tuple[str, CapitalBucket]:
        
        # REGIME 4: THE CRASH (Backwardation)
        # Term structure slope > 1.0 (Monthly IV < Weekly IV) implies panic.
        if m.term_structure_slope > 1.05:
            if ai_sentiment == "BULLISH": return "WAIT", CapitalBucket.INTRADAY
            return "BEAR_CALL_SPREAD", CapitalBucket.WEEKLY # Sell Calls into the crash

        # REGIME 5: THE RALLY (Bull Trend)
        # Standard trend following.
        if m.trend_status == "BULL_TREND" and m.ivp < 60:
            if ai_sentiment == "BEARISH": return "WAIT", CapitalBucket.INTRADAY
            return "BULL_PUT_SPREAD", CapitalBucket.WEEKLY

        # REGIME 1: THE GRIND (Low Vol + Skew)
        # IVP < 35. Premiums are cheap. Selling naked is dangerous (Gamma risk).
        if m.ivp < 35:
            # If Vol is suspiciously low, stay out (avoid "picking pennies in front of steamroller")
            if m.ivp < 15 and m.vrp_score < 0: return "WAIT", CapitalBucket.INTRADAY
            
            # If Bearish, don't try to capture premium.
            if ai_sentiment == "BEARISH": return "WAIT", CapitalBucket.INTRADAY
            
            # Jade Lizard: Attempt to capture skew with no upside risk.
            return "JADE_LIZARD", CapitalBucket.WEEKLY

        # REGIME 3: THE HARVEST (High Vol)
        # IVP > 50. This is the sweet spot for premium selling.
        if m.ivp > 50:
            # If we trust the data and AI isn't scared, go undefined risk (Strangle)
            if m.structure_confidence > 0.8 and ai_sentiment == "NEUTRAL":
                return "SHORT_STRANGLE", CapitalBucket.WEEKLY
            else:
                return "IRON_CONDOR", CapitalBucket.WEEKLY

        # REGIME 6: THE SQUEEZE (High Skew / Fear)
        # Skew > 5.0 means Puts are extremely expensive relative to Calls.
        # Use "Crash Defense": Sell the expensive Puts to finance an ATM Hedge.
        if m.volatility_skew > 5.0:
             return "RATIO_SPREAD_PUT", CapitalBucket.WEEKLY

        # Default State: Normal Market
        return "IRON_CONDOR", CapitalBucket.WEEKLY

    def _construct_legs(self, strat: str, m: AdvancedMetrics, spot: float) -> Tuple[List[Dict], ExpiryType]:
        expiries = self.master.get_all_expiries("NIFTY")
        if not expiries: return [], ExpiryType.WEEKLY
        near_exp = expiries[0].strftime("%Y-%m-%d")
        
        atm = round(spot / 50) * 50
        
        # Dynamic Widths based on VIX
        wing_width = 100 if m.vix < 15 else 200
        short_dist = 200 if m.vix < 15 else 400
        
        legs = []

        if strat == "JADE_LIZARD":
            # LOGIC: Sell OTM Put, Sell OTM Call Spread.
            # Safety: In low vol, we tighten the call spread to 50 to ensure Net Credit > Width.
            safe_wing_width = 50 if m.vix < 14 else 100
            
            legs = [
                # Protection First (Buy Leg)
                {"type": "CE", "strike": atm + short_dist + safe_wing_width, "side": "BUY", "expiry": near_exp},
                # Short Legs
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "CE", "strike": atm + short_dist, "side": "SELL", "expiry": near_exp}
            ]
            
        elif strat == "SHORT_STRANGLE":
            legs = [
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "CE", "strike": atm + short_dist, "side": "SELL", "expiry": near_exp}
            ]
            
        elif strat == "IRON_CONDOR":
            legs = [
                # Protection First (Wings)
                {"type": "CE", "strike": atm + short_dist + wing_width, "side": "BUY", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist - wing_width, "side": "BUY", "expiry": near_exp},
                # Body (Shorts)
                {"type": "CE", "strike": atm + short_dist, "side": "SELL", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp}
            ]
            
        elif strat == "BEAR_CALL_SPREAD":
             legs = [
                {"type": "CE", "strike": atm + 200, "side": "BUY", "expiry": near_exp},
                {"type": "CE", "strike": atm, "side": "SELL", "expiry": near_exp}
            ]
            
        elif strat == "BULL_PUT_SPREAD":
             legs = [
                {"type": "PE", "strike": atm - short_dist - 100, "side": "BUY", "expiry": near_exp},
                {"type": "PE", "strike": atm - short_dist, "side": "SELL", "expiry": near_exp}
            ]
            
        elif strat == "RATIO_SPREAD_PUT":
             # Crash Defense: Buy 1 ATM Put, Sell 2 OTM Puts.
             # Risk: Defined on upside, Undefined on massive crash (below 2nd put).
             # But we collect huge credit to finance the ATM hedge.
             legs = [
                {"type": "PE", "strike": atm, "side": "BUY", "expiry": near_exp},
                {"type": "PE", "strike": atm - 300, "side": "SELL", "expiry": near_exp},
                {"type": "PE", "strike": atm - 300, "side": "SELL", "expiry": near_exp}
            ]
            
        elif strat == "CALL_RATIO_SPREAD":
             legs = [
                {"type": "CE", "strike": atm, "side": "BUY", "expiry": near_exp},
                {"type": "CE", "strike": atm + 200, "side": "SELL", "expiry": near_exp},
                {"type": "CE", "strike": atm + 200, "side": "SELL", "expiry": near_exp}
            ]
            
        return legs, ExpiryType.WEEKLY
