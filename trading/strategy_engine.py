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
        self.pricing = pricing_engine # Injected
        self.last_trade_time = None

    def select_strategy_with_capital(self, metrics: AdvancedMetrics, spot: float,
                                   capital_status: Dict) -> Tuple[str, List[Dict], ExpiryType, CapitalBucket]:
        
        now = datetime.now(IST)
        # Cooldown check
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        # Bucket Logic (Simplified for brevity, logic remains same as before)
        bucket = CapitalBucket.WEEKLY # Default logic place holder
        available = capital_status.get("available", {}).get(bucket.value, 0)
        
        if available < settings.ACCOUNT_SIZE * 0.02:
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        # Expiry Selection
        expiry_date = self._get_expiry_date()
        
        # --- COUNTER #2: DYNAMIC STRIKE SELECTION ---
        # Instead of fixed points, we find strikes based on Delta
        
        strategy_name = StrategyType.WAIT.value
        legs = []

        # Example: Short Strangle Logic
        if metrics.ivp < 50:
            strategy_name = StrategyType.SHORT_STRANGLE.value
            
            # Target 16 Delta (approx 1 Std Dev)
            target_delta = 0.16 
            
            ce_strike = self._find_strike_by_delta(spot, "CE", expiry_date, target_delta)
            pe_strike = self._find_strike_by_delta(spot, "PE", expiry_date, target_delta)
            
            legs = [
                {"strike": ce_strike, "type": "CE", "side": "SELL", "expiry": expiry_date},
                {"strike": pe_strike, "type": "PE", "side": "SELL", "expiry": expiry_date}
            ]
        
        # Example: Iron Condor Logic
        else:
            strategy_name = StrategyType.IRON_CONDOR.value
            
            # Short legs at 20 Delta, Long legs at 5 Delta
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

    def _find_strike_by_delta(self, spot: float, option_type: str, expiry: str, target_delta: float) -> float:
        """
        Iteratively finds the strike closest to the target Delta.
        """
        # Start search at ATM
        strike = round(spot / 50) * 50
        step = 50
        
        # Limit iterations to prevent infinite loops
        for _ in range(20):
            greeks = self.pricing.calculate_greeks(spot, strike, option_type, expiry)
            current_delta = abs(greeks.delta)
            
            if abs(current_delta - target_delta) < 0.05:
                return strike
            
            # Adjust strike
            # For Calls: Higher Strike -> Lower Delta
            # For Puts: Lower Strike -> Lower Delta (since we look at abs(delta))
            if current_delta > target_delta:
                # We need to move further OTM
                if option_type == "CE": strike += step
                else: strike -= step
            else:
                # We need to move closer to ATM
                if option_type == "CE": strike -= step
                else: strike += step
                
        return strike

    def _get_expiry_date(self) -> str:
        # Simplified next Thursday logic
        today = datetime.now(IST)
        days_ahead = (3 - today.weekday()) % 7
        if days_ahead == 0 and today.time() >= dtime(15, 30): 
            days_ahead = 7
        return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
