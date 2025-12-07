import logging
import time
from datetime import datetime, timedelta, time as dtime
from typing import List, Dict, Tuple, Optional
from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import MarketRegime, StrategyType, ExpiryType, CapitalBucket
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("StrategyEngine")

class IntelligentStrategyEngine:
    """
    FIXED: Added timeout guards to Delta Search and oscillation detection for strike selection.
    FIXED: Added freeze quantity validation for exchange limits.
    FIXED: Uses real instrument expiries instead of assuming Thursday.
    """
    def __init__(self, vol_analytics, event_intel, capital_allocator, pricing_engine: HybridPricingEngine):
        self.vol_analytics = vol_analytics
        self.event_intel = event_intel
        self.capital_allocator = capital_allocator
        self.pricing = pricing_engine
        self.last_trade_time = None
        self.instruments_master = None  # Will be injected by Engine

    def set_instruments_master(self, master):
        """Inject InstrumentMaster for real expiry lookups"""
        self.instruments_master = master

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
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        # CRITICAL FIX: Get real expiry from Instrument Master
        expiry_date = self._get_expiry_date()
        if not expiry_date:
            logger.warning("⚠️ No valid expiry found. Waiting...")
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        strategy_name = StrategyType.WAIT.value
        legs = []

        try:
            # DYNAMIC POSITION SIZING with Freeze Quantity Check
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
        """
        FIXED: Scales position size based on available capital.
        Now respects exchange freeze quantity limits.
        """
        raw_lots = int(available_capital / margin_per_lot)
        
        # Cap by MAX_LOTS configuration
        capped_lots = min(raw_lots, settings.MAX_LOTS)
        
        # CRITICAL FIX: Cap by Exchange Freeze Quantity
        # NIFTY Lot Size = 75, Freeze Qty = 1800
        max_lots_per_freeze = settings.NIFTY_FREEZE_QTY // settings.LOT_SIZE  # = 24 lots
        
        final_lots = min(capped_lots, max_lots_per_freeze)
        
        if final_lots != raw_lots:
            logger.debug(
                f"Position Size Adjusted: Raw={raw_lots}, "
                f"Config Cap={settings.MAX_LOTS}, "
                f"Freeze Cap={max_lots_per_freeze}, "
                f"Final={final_lots}"
            )
        
        return final_lots

    def _find_strike_by_delta(self, spot: float, option_type: str, expiry: str, 
                              target_delta: float, max_iterations: int = 30) -> float:
        """
        FIXED: Binary Search for strike matching target Delta.
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
            # 1. Timeout Guard (2 seconds max)
            if time.time() - start_time > 2.0:
                logger.warning(
                    f"⏱️ Delta Search Timeout ({option_type}, target={target_delta:.2f}). "
                    f"Returning best match: {best_strike}"
                )
                return best_strike

            mid_strike = (lower_strike + upper_strike) / 2
            mid_strike = round(mid_strike / step) * step  # Snap to Grid
            
            # 2. Oscillation Guard
            # If search range is tighter than one strike step, we can't get closer
            if (upper_strike - lower_strike) < step:
                return best_strike

            greeks = self.pricing.calculate_greeks(spot, mid_strike, option_type, expiry)
            current_delta = abs(greeks.delta)
            error = abs(current_delta - target_delta)
            
            if error < best_error:
                best_error = error
                best_strike = mid_strike

            # Success tolerance (Delta is within 0.02)
            if error < 0.02: 
                return mid_strike
            
            # Adjust Bounds
            if option_type == "CE":
                # Call Delta increases as Strike decreases (ITM)
                if current_delta > target_delta: 
                    lower_strike = mid_strike 
                else: 
                    upper_strike = mid_strike 
            else:
                # Put Delta (abs) increases as Strike increases (ITM)
                if current_delta > target_delta: 
                    upper_strike = mid_strike 
                else: 
                    lower_strike = mid_strike

        # Check convergence quality
        if best_error > 0.10:
            logger.warning(
                f"⚠️ Poor Delta Match: Target={target_delta:.2f}, "
                f"Best Error={best_error:.3f}, Strike={best_strike}"
            )
            
        return best_strike

    def _get_expiry_date(self) -> Optional[str]:
        """
        CRITICAL FIX: Uses real instrument expiries from Instrument Master.
        Properly handles holidays and market closures.
        """
        # If InstrumentMaster is available, use real data
        if self.instruments_master:
            try:
                available_expiries = self.instruments_master.get_all_expiries("NIFTY")
                
                if not available_expiries:
                    logger.error("❌ No NIFTY expiries available in Instrument Master")
                    return None
                
                today = datetime.now(IST).date()
                
                # Get next valid expiry (future dates only)
                future_expiries = [e for e in available_expiries if e > today]
                
                if not future_expiries:
                    logger.error("❌ No future expiries available")
                    return None
                
                # Return nearest expiry
                nearest_expiry = future_expiries[0]
                logger.debug(f"📅 Next Expiry: {nearest_expiry}")
                return nearest_expiry.strftime("%Y-%m-%d")
                
            except Exception as e:
                logger.error(f"Expiry lookup failed: {e}")
                # Fall through to fallback logic
        
        # FALLBACK: Simple Thursday logic (if InstrumentMaster unavailable)
        logger.warning("⚠️ Using fallback expiry logic (Thursday assumption)")
        today = datetime.now(IST)
        
        # 3 = Thursday (Monday is 0)
        days_ahead = (3 - today.weekday()) % 7
        
        # If today is Thursday and market is closed (after 3:30 PM), move to next week
        if days_ahead == 0 and today.time() >= dtime(15, 30): 
            days_ahead = 7
            
        return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
