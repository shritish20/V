import logging
import time
from datetime import datetime, timedelta, time as dtime
from typing import List, Dict, Tuple, Optional
from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import StrategyType, ExpiryType, CapitalBucket

logger = logging.getLogger("StrategyEngine")

class IntelligentStrategyEngine:
    """
    VolGuard 2.0 Strategy Engine (Institutional Grade)
    Features:
    - Multi-Factor Consensus (GARCH vs IV, Skew, PCR)
    - Dynamic DTE Aggression (Theta Eater vs Gamma Guard)
    - "No Fly Zone" Strike Safety (Straddle Price)
    """
    def __init__(self, vol_analytics, event_intel, capital_allocator, pricing_engine):
        self.vol_analytics = vol_analytics
        self.event_intel = event_intel
        self.capital_allocator = capital_allocator
        self.pricing = pricing_engine
        self.instruments_master = None
        self.last_trade_time = None

    def set_instruments_master(self, master):
        self.instruments_master = master

    def select_strategy_with_capital(self, metrics: AdvancedMetrics, spot: float,
                                   capital_status: Dict) -> Tuple[str, List[Dict], ExpiryType, CapitalBucket]:
        
        now = datetime.now(IST)
        bucket = CapitalBucket.WEEKLY
        
        # 1. Cooldown (5 mins)
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # 2. Capital Check (Min 2% or 1.5L)
        available = capital_status.get("available", {}).get(bucket.value, 0)
        min_req = max(settings.ACCOUNT_SIZE * 0.02, 150000)
        if available < min_req:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # 3. Get Valid Expiry
        expiry_date = self._get_expiry_date(ExpiryType.WEEKLY)
        if not expiry_date:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # =========================================================
        # PHASE 1: THE QUANT FILTER (Value & Safety)
        # =========================================================
        
        # Safety Hard Stops
        dangerous_regimes = ["PANIC", "FEAR_BACKWARDATION"]
        if metrics.regime in dangerous_regimes:
            logger.warning(f"⛔ Strategy Hold: Market Regime is {metrics.regime}")
            return "WAIT", [], ExpiryType.WEEKLY, bucket
            
        if metrics.event_risk_score >= 3.0:
            logger.warning(f"⛔ Strategy Hold: High Event Risk ({metrics.event_risk_score})")
            return "WAIT", [], ExpiryType.WEEKLY, bucket
            
        if metrics.vix > 28.0:
             logger.warning(f"⛔ Strategy Hold: VIX {metrics.vix} too high")
             return "WAIT", [], ExpiryType.WEEKLY, bucket

        # Value Check: Is Volatility Cheap or Expensive? (IV vs GARCH)
        # Positive = Expensive (Sell). Negative = Cheap (Trap).
        # We look for a spread where Implied Vol is irrationally higher than Forecast
        vol_premium = metrics.vix - metrics.garch_vol_7d
        
        # Thresholds
        is_vol_expensive = vol_premium > 0.5   # Edge found
        is_vol_cheap = vol_premium < -1.5      # Trap found

        # =========================================================
        # PHASE 2: THE COMPASS (Directional Bias)
        # =========================================================
        bias = "NEUTRAL"
        
        # Put-Call Ratio Logic
        if metrics.pcr > 1.25: 
            bias = "BULLISH" # Strong Support
        elif metrics.pcr < 0.70: 
            bias = "BEARISH" # Strong Resistance

        # Optional: Gravity Check (Price vs Max Pain)
        # If Price is significantly far from Max Pain, it might snap back
        dist_pain = (spot - metrics.max_pain) / spot
        if abs(dist_pain) > 0.015 and bias == "NEUTRAL":
            if dist_pain > 0: bias = "BEARISH_LEAN" # Price too high
            else: bias = "BULLISH_LEAN" # Price too low

        # =========================================================
        # PHASE 3: THE MATRIX (Strategy Selection)
        # =========================================================
        strategy_name = "WAIT"

        # A. DIRECTIONAL (Trend is King)
        if bias in ["BULLISH", "BULLISH_LEAN"]:
            strategy_name = "BULL_PUT_SPREAD"
        
        elif bias in ["BEARISH", "BEARISH_LEAN"]:
            strategy_name = "BEAR_CALL_SPREAD"
        
        # B. NEUTRAL (Vol is King)
        else:
            # 1. Skew Exploit: Puts are insanely expensive? Sell Jade Lizard.
            # (Jade Lizard = Short Put + Bear Call Spread)
            # Only do this if Puts are expensive (High Skew) AND Vol is decent
            if metrics.volatility_skew > 4.0 and is_vol_expensive:
                strategy_name = "JADE_LIZARD"
            
            # 2. Income Harvest: Vol is expensive? Sell Strangle (Naked).
            elif is_vol_expensive:
                strategy_name = "SHORT_STRANGLE"
                
            # 3. Defensive: Vol is cheap (Trap) or Normal? Buy Iron Condor (Wings).
            else:
                strategy_name = "IRON_CONDOR"

        # =========================================================
        # PHASE 4: EXECUTION MAP (Strikes & DTE)
        # =========================================================
        
        # Calculate Days to Expiry (DTE)
        today_date = datetime.now(IST).date()
        exp_dt = datetime.strptime(expiry_date, "%Y-%m-%d").date()
        dte = (exp_dt - today_date).days

        # Generate Legs with Safety Zones
        legs = self._generate_smart_legs(strategy_name, spot, expiry_date, dte, metrics)
        
        if not legs:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # Final Sizing (Freeze Limits)
        lots = self._calculate_dynamic_lots(available, 150000.0)
        
        # Final Freeze Check
        if self._validate_freeze_limits(legs, lots):
             self.last_trade_time = now
             return strategy_name, legs, ExpiryType.WEEKLY, bucket
        else:
             # Try reducing lots if we hit freeze limit? 
             # For safety, we just WAIT and log error in validate function
             return "WAIT", [], ExpiryType.WEEKLY, bucket

    def _generate_smart_legs(self, strategy_name: str, spot: float, expiry: str, dte: int, metrics: AdvancedMetrics) -> List[Dict]:
        """
        Generates legs using 'Theta Eater' logic (DTE) and 'Safety Zones' (Straddle Price).
        """
        legs = []
        
        # 1. The Gamma Guard (DTE Logic)
        # Mon/Tue (DTE > 2): Aggressive 30 Delta (Eat Theta)
        # Wed/Thu (DTE <= 2): Defensive 20 Delta (Avoid Gamma)
        target_delta = 0.30 if dte > 2 else 0.20
        
        # 2. The Safety Map (No Fly Zone)
        # Never sell inside the expected move
        expected_move = metrics.straddle_price if metrics.straddle_price > 0 else (spot * 0.01)
        safe_floor = spot - (expected_move * 1.05) # 5% Buffer
        safe_ceiling = spot + (expected_move * 1.05)
        
        width = 200.0 # Wing Width for spreads

        try:
            # --- NEUTRAL: SHORT STRANGLE (Naked) ---
            if strategy_name == "SHORT_STRANGLE":
                # For naked, we are always a bit safer unless GARCH screams "FREE MONEY"
                delta = 0.16 
                ce = self._find_strike_by_delta(spot, "CE", expiry, delta)
                pe = self._find_strike_by_delta(spot, "PE", expiry, delta)
                
                # Push out if inside safety zone
                final_ce = max(ce, safe_ceiling)
                final_pe = min(pe, safe_floor)
                
                legs = [
                    {"strike": final_ce, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": final_pe, "type": "PE", "side": "SELL", "expiry": expiry}
                ]

            # --- NEUTRAL: IRON CONDOR (Defined Risk) ---
            elif strategy_name == "IRON_CONDOR":
                # Sell at Target Delta (20 or 30)
                raw_ce = self._find_strike_by_delta(spot, "CE", expiry, target_delta)
                raw_pe = self._find_strike_by_delta(spot, "PE", expiry, target_delta)
                
                # Enforce Safety Zone
                ce_sell = max(raw_ce, safe_ceiling)
                pe_sell = min(raw_pe, safe_floor)
                
                legs = [
                    {"strike": ce_sell, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": ce_sell + width, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": pe_sell, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": pe_sell - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

            # --- BULLISH: BULL PUT SPREAD ---
            elif strategy_name == "BULL_PUT_SPREAD":
                # Sell Put (Bullish). We can be aggressive on Delta (target_delta)
                raw_pe = self._find_strike_by_delta(spot, "PE", expiry, target_delta)
                
                # Safety: Don't sell above floor (too close to money)
                pe_sell = min(raw_pe, safe_floor) 
                
                legs = [
                    {"strike": pe_sell, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": pe_sell - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

            # --- BEARISH: BEAR CALL SPREAD ---
            elif strategy_name == "BEAR_CALL_SPREAD":
                # Sell Call (Bearish)
                raw_ce = self._find_strike_by_delta(spot, "CE", expiry, target_delta)
                
                # Safety: Don't sell below ceiling
                ce_sell = max(raw_ce, safe_ceiling)
                
                legs = [
                    {"strike": ce_sell, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": ce_sell + width, "type": "CE", "side": "BUY", "expiry": expiry}
                ]

            # --- SPECIAL: JADE LIZARD ---
            elif strategy_name == "JADE_LIZARD":
                # 1. Sell Big Put (Aggressive 30 Delta) - The Income Engine
                # Jade Lizard relies on Put Credit to offset Call risk.
                raw_pe = self._find_strike_by_delta(spot, "PE", expiry, 0.30)
                pe_sell = min(raw_pe, safe_floor)
                
                # 2. Sell Bear Call Spread (Conservative 15 Delta)
                ce_sell = max(self._find_strike_by_delta(spot, "CE", expiry, 0.15), safe_ceiling)
                
                legs = [
                    {"strike": pe_sell, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": ce_sell, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": ce_sell + width, "type": "CE", "side": "BUY", "expiry": expiry}
                ]

        except Exception as e:
            logger.error(f"Leg Generation Error: {e}")
            return []

        return legs

    # --- HELPER METHODS ---
    
    def _calculate_dynamic_lots(self, available, margin_per_lot):
        raw_lots = int(available / margin_per_lot)
        # 1. Cap by Config Max
        capped_lots = min(raw_lots, settings.MAX_LOTS)
        # 2. Cap by Exchange Freeze (1800 qty / 75 lot = 24 lots)
        freeze_cap = settings.NIFTY_FREEZE_QTY // settings.LOT_SIZE
        return min(capped_lots, freeze_cap)

    def _validate_freeze_limits(self, legs, lots):
        total_qty = lots * settings.LOT_SIZE
        if total_qty > settings.NIFTY_FREEZE_QTY:
            logger.error(f"🚫 Freeze Limit Breach: {total_qty} > {settings.NIFTY_FREEZE_QTY}")
            return False
        return True

    def _find_strike_by_delta(self, spot: float, option_type: str, expiry: str, target_delta: float) -> float:
        """
        Binary Search for strike matching target Delta.
        """
        step = 50.0
        # Initial guess based on spot
        best_strike = round(spot / step) * step
        best_error = float('inf')
        
        # Define a search window (+/- 20% from spot)
        lower_bound = int(spot * 0.8)
        upper_bound = int(spot * 1.2)
        
        # Iterate through strikes
        # Note: In production, you might optimize this to not loop every 50pts, 
        # but for Nifty, looping 100 strikes is very fast.
        for strike in range(lower_bound, upper_bound, int(step)):
            greeks = self.pricing.calculate_greeks(spot, strike, option_type, expiry)
            
            current_delta = abs(greeks.delta) if greeks.delta is not None else 0.0
            error = abs(current_delta - target_delta)
            
            if error < best_error:
                best_error = error
                best_strike = strike
        
        return float(best_strike)

    def _get_expiry_date(self, expiry_type: ExpiryType) -> Optional[str]:
        # Rely on Instrument Master
        if self.instruments_master:
            try:
                # Get all NIFTY expiries sorted
                expiries = self.instruments_master.get_all_expiries("NIFTY")
                if not expiries:
                    return None
                
                # Logic for Weekly: Just return the nearest one
                # Logic for Monthly: Return last Thursday of month
                # For simplicity in this engine, we default to nearest available (Weekly logic)
                # You can expand this if you specifically want Monthly buckets.
                
                return expiries[0].strftime("%Y-%m-%d")
            except Exception as e:
                logger.error(f"Expiry Lookup Failed: {e}")
                return None
        return None
