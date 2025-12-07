import logging
from datetime import datetime, timedelta, time as dtime
from typing import List, Dict, Tuple, Optional
from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import MarketRegime, StrategyType, ExpiryType, CapitalBucket
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("VolGuard17")

class IntelligentStrategyEngine:
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
        
        if available < settings.ACCOUNT_SIZE * 0.02:
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        expiry_date = self._get_expiry_date()
        strategy_name = StrategyType.WAIT.value
        legs = []

        try:
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

    def _find_strike_by_delta(self, spot: float, option_type: str, expiry: str, 
                              target_delta: float, max_iterations: int = 30) -> float:
        """
        Binary Search for strike matching target Delta.
        """
        # Search bounds: +/- 40% from spot
        lower_strike = spot * 0.6
        upper_strike = spot * 1.4
        
        best_strike = spot
        best_error = float('inf')

        for _ in range(max_iterations):
            mid_strike = (lower_strike + upper_strike) / 2
            mid_strike = round(mid_strike / 50) * 50  # Round to Nifty Step
            
            greeks = self.pricing.calculate_greeks(spot, mid_strike, option_type, expiry)
            current_delta = abs(greeks.delta)
            error = abs(current_delta - target_delta)
            
            if error < best_error:
                best_error = error
                best_strike = mid_strike

            # Success tolerance
            if error < 0.02: 
                return mid_strike
            
            # Adjust Bounds
            if option_type == "CE":
                # Call: Higher Strike = Lower Delta
                if current_delta > target_delta: lower_strike = mid_strike # Need higher strike
                else: upper_strike = mid_strike 
            else:
                # Put: Lower Strike = Lower Delta (abs)
                if current_delta > target_delta: upper_strike = mid_strike # Need lower strike
                else: lower_strike = mid_strike

        # Check convergence
        if best_error > 0.10:
            raise RuntimeError(f"Could not find strike for delta {target_delta}. Best err: {best_error}")
            
        return best_strike

    def _get_expiry_date(self) -> str:
        today = datetime.now(IST)
        days_ahead = (3 - today.weekday()) % 7
        if days_ahead == 0 and today.time() >= dtime(15, 30): 
            days_ahead = 7
        return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

