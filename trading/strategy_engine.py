import logging
import time
from datetime import datetime, timedelta, time as dtime
from typing import List, Dict, Tuple, Optional
from core.config import settings, IST
from core.models import AdvancedMetrics, MultiLegTrade
from core.enums import MarketRegime, StrategyType, ExpiryType, CapitalBucket
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("StrategyEngine")

class IntelligentStrategyEngine:
    """
    PRODUCTION FIXED v2.0:
    - Uses SAFE_TRADE_END (3:15 PM) to prevent 0DTE trades near close
    - Proper expiry date logic with holiday handling
    - Per-leg freeze quantity validation
    - Strike selection timeout guards
    - Real instrument expiry integration
    """
    def __init__(self, vol_analytics, event_intel, capital_allocator, pricing_engine: HybridPricingEngine):
        self.vol_analytics = vol_analytics
        self.event_intel = event_intel
        self.capital_allocator = capital_allocator
        self.pricing = pricing_engine
        self.last_trade_time = None
        self.instruments_master = None

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

        # PRODUCTION FIX: Get real expiry with proper edge case handling
        expiry_date = self._get_expiry_date(ExpiryType.WEEKLY)
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

            # PRODUCTION FIX: Validate freeze quantities per-leg
            if not self._validate_freeze_limits(legs, lots):
                logger.error("🚫 Freeze limit validation failed. Reducing lots...")
                # Try with fewer lots
                safe_lots = self._calculate_safe_lots_for_freeze()
                if safe_lots < 1:
                    return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, bucket
                lots = safe_lots

            self.last_trade_time = now
            return strategy_name, legs, ExpiryType.WEEKLY, bucket

        except RuntimeError as e:
            logger.warning(f"Strategy Selection Failed: {e}")
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, bucket

    def _calculate_dynamic_lots(self, available_capital: float, margin_per_lot: float) -> int:
        """
        PRODUCTION FIX: Scales position size based on available capital.
        Respects MAX_LOTS and exchange freeze quantity limits.
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

    def _validate_freeze_limits(self, legs: List[Dict], lots: int) -> bool:
        """
        PRODUCTION FIX: Validates each leg against exchange freeze quantity limits.
        """
        for leg in legs:
            total_qty = lots * settings.LOT_SIZE
            
            if total_qty > settings.NIFTY_FREEZE_QTY:
                logger.error(
                    f"🚫 Freeze Limit Breach: {leg['side']} {leg['strike']} {leg['type']} "
                    f"has {total_qty} qty > {settings.NIFTY_FREEZE_QTY} limit"
                )
                return False
        
        return True

    def _calculate_safe_lots_for_freeze(self) -> int:
        """
        Calculate maximum safe lots that won't violate freeze limits.
        """
        return settings.NIFTY_FREEZE_QTY // settings.LOT_SIZE

    def _find_strike_by_delta(self, spot: float, option_type: str, expiry: str, 
                              target_delta: float, max_iterations: int = 30) -> float:
        """
        PRODUCTION FIX: Binary Search for strike matching target Delta.
        Includes Timeouts and Oscillation Detection.
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
            mid_strike = round(mid_strike / step) * step
            
            # 2. Oscillation Guard
            if (upper_strike - lower_strike) < step:
                return best_strike

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
                if current_delta > target_delta: 
                    lower_strike = mid_strike 
                else: 
                    upper_strike = mid_strike 
            else:
                if current_delta > target_delta: 
                    upper_strike = mid_strike 
                else: 
                    lower_strike = mid_strike

        if best_error > 0.10:
            logger.warning(
                f"⚠️ Poor Delta Match: Target={target_delta:.2f}, "
                f"Best Error={best_error:.3f}, Strike={best_strike}"
            )
            
        return best_strike

    def _get_expiry_date(self, expiry_type: ExpiryType = ExpiryType.WEEKLY) -> Optional[str]:
        """
        PRODUCTION FIX v2.0: 
        - Uses SAFE_TRADE_END (3:15 PM) instead of market close (3:30 PM)
        - Handles monthly expiries, holidays, and 0DTE correctly
        - Prevents trades in last 15 minutes of trading day
        """
        if self.instruments_master:
            try:
                available_expiries = self.instruments_master.get_all_expiries("NIFTY")
                
                if not available_expiries:
                    logger.error("❌ No NIFTY expiries available in Instrument Master")
                    return None
                
                today = datetime.now(IST)
                current_time = today.time()
                today_date = today.date()
                
                # CRITICAL FIX: Use SAFE_TRADE_END (3:15 PM) instead of market close (3:30 PM)
                # This prevents 0DTE trades in the last 15 minutes when gamma risk is extreme
                if current_time < settings.SAFE_TRADE_END:
                    # Market still open AND before safe cutoff, today's expiry is valid
                    future_expiries = [e for e in available_expiries if e >= today_date]
                else:
                    # After 3:15 PM or market closed, exclude today
                    future_expiries = [e for e in available_expiries if e > today_date]
                    logger.debug(f"⏰ After {settings.SAFE_TRADE_END.strftime('%H:%M')} - excluding today's expiry")
                
                if not future_expiries:
                    logger.error("❌ No future expiries available")
                    return None
                
                if expiry_type == ExpiryType.WEEKLY:
                    # Return nearest expiry (weekly contract)
                    nearest_expiry = future_expiries[0]
                    logger.debug(f"📅 Next Weekly Expiry: {nearest_expiry}")
                    return nearest_expiry.strftime("%Y-%m-%d")
                
                elif expiry_type == ExpiryType.MONTHLY:
                    # Find last Thursday of current month
                    current_month_expiries = [
                        e for e in future_expiries 
                        if e.month == today.month and e.year == today.year
                    ]
                    
                    if not current_month_expiries:
                        # No expiries left this month, get first expiry of next month
                        next_month_expiries = [
                            e for e in future_expiries 
                            if e.month == (today.month % 12) + 1
                        ]
                        if next_month_expiries:
                            monthly_expiry = next_month_expiries[-1]
                        else:
                            monthly_expiry = future_expiries[-1]
                    else:
                        # Get last expiry of current month
                        monthly_expiry = current_month_expiries[-1]
                    
                    logger.debug(f"📅 Next Monthly Expiry: {monthly_expiry}")
                    return monthly_expiry.strftime("%Y-%m-%d")
                
                elif expiry_type == ExpiryType.INTRADAY:
                    # For intraday, use nearest expiry
                    return future_expiries[0].strftime("%Y-%m-%d")
                    
            except Exception as e:
                logger.error(f"Expiry lookup failed: {e}")
                # Fall through to fallback
        
        # FALLBACK: Simple Thursday logic
        logger.warning("⚠️ Using fallback expiry logic (InstrumentMaster unavailable)")
        today = datetime.now(IST)
        
        # Calculate days to Thursday (weekday 3)
        days_ahead = (3 - today.weekday()) % 7
        
        # CRITICAL FIX: Use SAFE_TRADE_END instead of market close
        if days_ahead == 0:
            if today.time() >= settings.SAFE_TRADE_END:
                # After safe cutoff, move to next Thursday
                days_ahead = 7
            # else: keep days_ahead = 0 (today is valid and before cutoff)
        
        target_date = today + timedelta(days=days_ahead)
        
        # For monthly expiry, get last Thursday of month
        if expiry_type == ExpiryType.MONTHLY:
            # Move to end of month
            next_month = target_date.replace(day=28) + timedelta(days=4)
            last_day = next_month - timedelta(days=next_month.day)
            
            # Find last Thursday
            while last_day.weekday() != 3:  # 3 = Thursday
                last_day -= timedelta(days=1)
            
            target_date = last_day
        
        return target_date.strftime("%Y-%m-%d")
