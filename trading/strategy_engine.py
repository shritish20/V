from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from core.models import AdvancedMetrics, MultiLegTrade, Position, GreeksSnapshot
from core.enums import MarketRegime
from core.config import IST, LOT_SIZE
from analytics.volatility import HybridVolatilityAnalytics
from analytics.events import AdvancedEventIntelligence
from datetime import time as dtime
import logging

logger = logging.getLogger("VolGuard14")

class AdvancedStrategyEngine:
    """Advanced strategy selection with regime awareness and analytics fusion - FIXED"""
    
    def __init__(self, volatility_analytics: HybridVolatilityAnalytics, event_intel: AdvancedEventIntelligence):
        self.vol_analytics = volatility_analytics
        self.event_intel = event_intel
        self.last_trade_time = None

    def select_strategy(self, metrics: AdvancedMetrics, spot: float) -> Tuple[str, List[Dict]]:
        """Select optimal strategy based on comprehensive market regime analysis"""
        now = datetime.now(IST)
        
        # Trade cooldown period
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return "WAIT", []
        
        self.last_trade_time = now
        expiry = self._get_weekly_expiry()
        atm_strike = round(spot / 50) * 50 

        # Enhanced regime-based strategy selection
        if metrics.regime in [MarketRegime.PANIC, MarketRegime.FEAR_BACKWARDATION, MarketRegime.DEFENSIVE_EVENT]:
            return self._defensive_strategies(atm_strike, expiry, metrics)
        elif metrics.regime in [MarketRegime.CALM_COMPRESSION, MarketRegime.LOW_VOL_COMPRESSION]:
            return self._premium_selling_strategies(atm_strike, expiry, metrics)
        elif metrics.regime == MarketRegime.BULL_EXPANSION:
            return self._bullish_strategies(atm_strike, expiry, metrics)
        else:
            return self._neutral_strategies(atm_strike, expiry, metrics)

    def _defensive_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Defensive strategies for high volatility regimes"""
        if metrics.event_risk_score > 2.0:
            return (
                "DEFENSIVE_IRON_CONDOR",
                [
                    {"strike": atm + 600, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm + 800, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": atm - 600, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 800, "type": "PE", "side": "BUY", "expiry": expiry},
                ]
            )
        else:
            return (
                "DEFENSIVE_PUT_SPREAD",
                [
                    {"strike": atm - 200, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 400, "type": "PE", "side": "BUY", "expiry": expiry},
                ]
            )

    def _premium_selling_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Premium selling strategies for low volatility regimes"""
        if metrics.ivp < 30:
            # More aggressive in very low IV
            return (
                "SHORT_STRANGLE",
                [
                    {"strike": atm + 200, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 200, "type": "PE", "side": "SELL", "expiry": expiry},
                ]
            )
        else:
            # Conservative in moderate IV
            return (
                "IRON_CONDOR",
                [
                    {"strike": atm + 300, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm + 500, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": atm - 300, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm - 500, "type": "PE", "side": "BUY", "expiry": expiry},
                ]
            )

    def _bullish_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Bullish strategies (Bull Put Spread)"""
        return (
            "BULL_PUT_SPREAD",
            [
                {"strike": atm - 100, "type": "PE", "side": "SELL", "expiry": expiry},
                {"strike": atm - 300, "type": "PE", "side": "BUY", "expiry": expiry},
            ]
        )

    def _neutral_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Neutral strategies for transition regimes"""
        return (
            "NEUTRAL_IRON_CONDOR",
            [
                {"strike": atm + 400, "type": "CE", "side": "SELL", "expiry": expiry},
                {"strike": atm + 600, "type": "CE", "side": "BUY", "expiry": expiry},
                {"strike": atm - 400, "type": "PE", "side": "SELL", "expiry": expiry},
                {"strike": atm - 600, "type": "PE", "side": "BUY", "expiry": expiry},
            ]
        )

    def _get_weekly_expiry(self) -> str:
        """Get next weekly expiry (Thursday)"""
        today = datetime.now(IST)
        days_ahead = (3 - today.weekday()) % 7 
        if days_ahead == 0 and today.time() >= dtime(15, 30):
            days_ahead = 7
        expiry = today + timedelta(days=days_ahead)
        return expiry.strftime("%Y-%m-%d")

    def get_strategy_metrics(self, strategy: str, legs_spec: List[Dict], spot: float) -> Dict[str, Any]:
        """Calculate comprehensive metrics for a strategy"""
        # This would calculate expected max loss, probability of profit, etc.
        # Simplified for now
        return {
            "strategy": strategy,
            "expected_max_loss": 1000,  # Placeholder
            "probability_of_profit": 0.65,  # Placeholder
            "risk_reward_ratio": 2.5,  # Placeholder
            "margin_required": 50000  # Placeholder
                             }
