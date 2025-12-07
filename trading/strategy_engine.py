import logging
import time
from datetime import datetime, timedelta, time as dtime
from typing import List, Dict, Tuple, Optional
from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import MarketRegime, StrategyType, ExpiryType, CapitalBucket
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("VolGuard17")

class IntelligentStrategyEngine:
    """
    FIXED: Added timeout guards to Delta Search and oscillation detection for strike selection.
    Addresses Medium Priority Issue #8: "Strategy Engine Delta Targeting Can Infinite Loop"
    """
    def __init__(self, vol_analytics, event_intel, capital_allocator, pricing_engine: HybridPricingEngine):
        self.vol_analytics = vol_analytics
        self.event_intel = event_intel
        self.capital_allocator = capital_allocator
        self.pricing = pricing_engine
        self.last_trade_time = None

    def select_strategy_with_capital(self, metrics: AdvancedMetrics, spot: float,
                                   capital_status: Dict) -> Tuple[str, List[Dict], ExpiryType, CapitalBucket]:
        
        now = datetime.now(IST)
        # Cooldown check (5 minutes)
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        bucket = CapitalBucket.WEEKLY 
        available = capital_status.get("available", {}).get(bucket.value, 0)
        
        # Require at least 2% of account size or enough for 1 lot (~1.5L)
        min_required = max(settings.ACCOUNT_SIZE * 0.02, 150000)
        if available < min_required:
            # logger.debug(f"Insufficient capital: {available} < {min_required}")
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        expiry_date = self._get_expiry_date()
        strategy_name = StrategyType.WAIT.value
        legs = []

        try:
            # DYNAMIC POSITION SIZING
            # Calculate how many lots we can afford (conservative: 1.5L per lot margin)
            lots = self._calculate_dynamic_lots(available, 150000.0)
            if lots < 1: 
                return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

            # STRATEGY LOGIC
            if metrics.ivp < 50:
                strategy_name = StrategyType.SHORT_STRANGLE.value
                target_delta = 0.16 
                
                ce_strike = self._find_strike_by_delta(spot, "CE", expiry_date, target_delta)
                pe_strike = self._find_strike_by_delta(spot, "PE", expiry_date, target_delta)
                
                legs = [
                    {"strike": ce_strike, "type": "CE", "side": "SELL", "expiry": expiry_date},
                    {"strike": pe_strike, "type": "PE", "side": "SELL", "expiry": expiry_date}
                ]
            else:
                strategy_name = StrategyType.IRON_CONDOR.value
                short_delta = 0.20
                long_delta = 0.05
                
                ce_short = self._find_strike_by_delta(spot, "CE", expiry_date, short_delta)
                pe_short = self._find_strike_by_delta(spot, "PE", expiry_date, short_delta)
                ce_long = self._find_strike_by_delta(spot, "CE", expiry_date, long_delta)
                pe_long = self._find_strike_by_delta(spot, "PE", expiry_date, long_delta)
                
                legs = [
                    {"strike": ce_short, "type": "CE", "side": "SELL", "expiry": expiry_date},
                    {"strike": ce_long,  "type": "CE", "side": "BUY",  "expiry": expiry_date},
                    {"strike": pe_short, "type": "PE", "side": "SELL", "expiry": expiry_date},
                    {"strike": pe_long,  "type": "PE", "side": "BUY",  "expiry": expiry_date}
                ]

            self.last_trade_time = now
            return strategy_name, legs, ExpiryType.WEEKLY, bucket

        except RuntimeError as e:
            logger.warning(f"Strategy Selection Failed: {e}")
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, bucket

    def _calculate_dynamic_lots(self, available_capital: float, margin_per_lot: float) -> int:
        """Scales position size based on available capital, capped by global Max Lots."""
        raw_lots = int(available_capital / margin_per_lot)
        return min(raw_lots, settings.MAX_LOTS)

    def _find_strike_by_delta(self, spot: float, option_type: str, expiry: str, 
                              target_delta: float, max_iterations: int = 30) -> float:
        """
        Binary Search for strike matching target Delta.
        Includes Timeouts and Oscillation Detection for robustness.
        """
        start_time = time.time()
        
        # Search bounds: +/- 40% from spot
        lower_strike = spot * 0.6
        upper_strike = spot * 1.4
        
        best_strike = spot
        best_error = float('inf')
        
        # Nifty strike step
        step = 50.0

        for i in range(max_iterations):
            # 1. Timeout Guard
            if time.time() - start_time > 2.0:
                logger.warning(f"Delta Search Timeout ({option_type}). Returning best match: {best_strike}")
                return best_strike

            mid_strike = (lower_strike + upper_strike) / 2
            mid_strike = round(mid_strike / step) * step  # Snap to Grid
            
            # 2. Oscillation Guard
            # If search range is tighter than one strike step, we can't get closer
            if (upper_strike - lower_strike) < step:
                # logger.debug(f"Delta Search Converged (Range Tight).")
                return best_strike

            greeks = self.pricing.calculate_greeks(spot, mid_strike, option_type, expiry)
            current_delta = abs(greeks.delta)
            error = abs(current_delta - target_delta)
            
            if error < best_error:
                best_error = error
                best_strike = mid_strike

            # Success tolerance (Delta is within 0.02, e.g., 0.14 to 0.18 for target 0.16)
            if error < 0.02: 
                return mid_strike
            
            # Adjust Bounds
            if option_type == "CE":
                # Call Delta is positive and increases as Strike decreases (ITM)
                # If current delta > target, we are too ITM (Strike too low) -> Increase Strike
                if current_delta > target_delta: 
                    lower_strike = mid_strike 
                else: 
                    upper_strike = mid_strike 
            else:
                # Put Delta is negative (using abs here)
                # Put Delta increases (abs) as Strike increases (ITM)
                # If current delta > target, we are too ITM (Strike too high) -> Decrease Strike
                if current_delta > target_delta: 
                    upper_strike = mid_strike 
                else: 
                    lower_strike = mid_strike

        # Check convergence quality
        if best_error > 0.10:
            logger.warning(f"Poor Delta Match: Target {target_delta}, Found {best_error:.3f} off. Using {best_strike}")
            
        return best_strike

    def _get_expiry_date(self) -> str:
        # Simple Weekly Expiry Logic (Next Thursday)
        # TODO: Replace with Real Holiday Calendar Check in future
        today = datetime.now(IST)
        
        # 3 = Thursday (Monday is 0)
        days_ahead = (3 - today.weekday()) % 7
        
        # If today is Thursday and market is closed (after 3:30 PM), move to next week
        if days_ahead == 0 and today.time() >= dtime(15, 30): 
            days_ahead = 7
            
        # If today is Thursday and market is open, use today (0 days ahead)
        
        return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
