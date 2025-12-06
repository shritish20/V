import logging
from datetime import datetime, timedelta, time as dtime
from typing import List, Dict, Tuple, Optional
from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import MarketRegime, StrategyType, ExpiryType, CapitalBucket
from analytics.volatility import HybridVolatilityAnalytics
from analytics.events import AdvancedEventIntelligence
from capital.allocator import SmartCapitalAllocator

logger = logging.getLogger("VolGuard17")

class IntelligentStrategyEngine:
    """
    VolGuard 19.0 Hybrid: 
    Restored V18 Logic (Regimes, Events, Skew) + V19 Capital Awareness.
    """
    def __init__(self, volatility_analytics: HybridVolatilityAnalytics,
                 event_intel: AdvancedEventIntelligence,
                 capital_allocator: SmartCapitalAllocator):
        self.vol_analytics = volatility_analytics
        self.event_intel = event_intel
        self.capital_allocator = capital_allocator
        self.last_trade_time = None
        self.strategy_history: List[Dict] = []

    def select_strategy_with_capital(self, metrics: AdvancedMetrics, spot: float,
                                   capital_status: Dict) -> Tuple[str, List[Dict], ExpiryType, CapitalBucket]:
        
        now = datetime.now(IST)
        
        # 1. Cooldown Check
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        # 2. Capital Bucket Availability Check
        available_buckets = []
        for bucket in CapitalBucket:
            available = capital_status.get("available", {}).get(bucket.value, 0)
            if available > settings.ACCOUNT_SIZE * 0.02: # Min 2% required
                available_buckets.append(bucket)

        if not available_buckets:
            logger.debug("No capital available in any bucket")
            return StrategyType.WAIT.value, [], ExpiryType.WEEKLY, CapitalBucket.WEEKLY

        # 3. Intelligent Bucket Selection (V18 Logic)
        selected_bucket = self._select_capital_bucket(metrics, available_buckets)
        expiry_type = self._get_expiry_type_for_bucket(selected_bucket)
        expiry = self._get_expiry_for_bucket(selected_bucket)
        
        # 4. ATM Strike Calculation
        atm_strike = round(spot / 50) * 50

        # 5. Regime-Based Strategy Selection (V18 Logic)
        if selected_bucket == CapitalBucket.WEEKLY:
            strategy_name, legs_spec = self._weekly_strategies(atm_strike, expiry, metrics)
        elif selected_bucket == CapitalBucket.MONTHLY:
            strategy_name, legs_spec = self._monthly_strategies(atm_strike, expiry, metrics)
        else:
            strategy_name, legs_spec = self._intraday_strategies(atm_strike, expiry, metrics)

        self.last_trade_time = now
        return strategy_name, legs_spec, expiry_type, selected_bucket

    def _select_capital_bucket(self, metrics: AdvancedMetrics, available_buckets: List[CapitalBucket]) -> CapitalBucket:
        # High Vol/Panic -> Prefer Weekly (Quick in/out)
        if metrics.regime in [MarketRegime.PANIC, MarketRegime.FEAR_BACKWARDATION, MarketRegime.DEFENSIVE_EVENT]:
            if CapitalBucket.WEEKLY in available_buckets:
                return CapitalBucket.WEEKLY
        
        # Low Vol -> Prefer Monthly (Premium Collection)
        elif metrics.regime in [MarketRegime.LOW_VOL_COMPRESSION, MarketRegime.CALM_COMPRESSION]:
            if CapitalBucket.MONTHLY in available_buckets:
                return CapitalBucket.MONTHLY
                
        # Default fallback
        return available_buckets[0]

    def _weekly_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        # V18 Logic: High Event Risk -> Defensive IC
        if metrics.event_risk_score > 2.5:
            return (
                StrategyType.DEFENSIVE_IRON_CONDOR.value,
                [
                    {"strike": atm + 400, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm + 600, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": atm - 400, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 600, "type": "PE", "side": "BUY", "expiry": expiry},
                ]
            )
        # V18 Logic: Low IV -> Short Strangle (Aggressive)
        elif metrics.ivp < 30:
            return (
                StrategyType.SHORT_STRANGLE.value,
                [
                    {"strike": atm + 200, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 200, "type": "PE", "side": "SELL", "expiry": expiry},
                ]
            )
        # Default: Iron Condor
        return (
            StrategyType.IRON_CONDOR.value,
            [
                {"strike": atm + 300, "type": "CE", "side": "SELL", "expiry": expiry},
                {"strike": atm + 500, "type": "CE", "side": "BUY", "expiry": expiry},
                {"strike": atm - 300, "type": "PE", "side": "SELL", "expiry": expiry},
                {"strike": atm - 500, "type": "PE", "side": "BUY", "expiry": expiry},
            ]
        )

    def _monthly_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        # V18 Logic: Bull Market -> Put Spread
        if metrics.regime == MarketRegime.BULL_EXPANSION:
            return (
                StrategyType.BULL_PUT_SPREAD.value,
                [
                    {"strike": atm - 400, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 600, "type": "PE", "side": "BUY", "expiry": expiry},
                ]
            )
        # Default: Wide Iron Condor
        return (
            StrategyType.IRON_CONDOR.value,
            [
                {"strike": atm + 500, "type": "CE", "side": "SELL", "expiry": expiry},
                {"strike": atm + 700, "type": "CE", "side": "BUY", "expiry": expiry},
                {"strike": atm - 500, "type": "PE", "side": "SELL", "expiry": expiry},
                {"strike": atm - 700, "type": "PE", "side": "BUY", "expiry": expiry},
            ]
        )

    def _intraday_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        # Intraday logic uses current day expiry usually
        today = datetime.now(IST).strftime("%Y-%m-%d")
        
        if metrics.ivp < 40:
            return (
                StrategyType.SHORT_STRANGLE.value,
                [
                    {"strike": atm + 100, "type": "CE", "side": "SELL", "expiry": today},
                    {"strike": atm - 100, "type": "PE", "side": "SELL", "expiry": today},
                ]
            )
        return (
            StrategyType.IRON_CONDOR.value,
            [
                {"strike": atm + 150, "type": "CE", "side": "SELL", "expiry": today},
                {"strike": atm + 250, "type": "CE", "side": "BUY", "expiry": today},
                {"strike": atm - 150, "type": "PE", "side": "SELL", "expiry": today},
                {"strike": atm - 250, "type": "PE", "side": "BUY", "expiry": today},
            ]
        )

    def _get_expiry_type_for_bucket(self, bucket: CapitalBucket) -> ExpiryType:
        if bucket == CapitalBucket.WEEKLY: return ExpiryType.WEEKLY
        elif bucket == CapitalBucket.MONTHLY: return ExpiryType.MONTHLY
        return ExpiryType.INTRADAY

    def _get_expiry_for_bucket(self, bucket: CapitalBucket) -> str:
        today = datetime.now(IST)
        if bucket == CapitalBucket.WEEKLY:
            days_ahead = (3 - today.weekday()) % 7
            if days_ahead == 0 and today.time() >= dtime(15, 30): days_ahead = 7
            return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        elif bucket == CapitalBucket.MONTHLY:
            # Simplified monthly calc
            next_month = today.replace(day=28) + timedelta(days=4)
            return (next_month - timedelta(days=(next_month.weekday() - 3) % 7)).strftime("%Y-%m-%d")
        return today.strftime("%Y-%m-%d")

