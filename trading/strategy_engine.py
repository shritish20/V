from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from core.models import AdvancedMetrics
from core.enums import MarketRegime
from core.config import IST, MARKET_KEY_INDEX
from datetime import time as dtime

class StrategyEngine:
    """Advanced strategy selection with regime awareness"""
    def __init__(self, analytics, event_intel):
        self.analytics = analytics
        self.event_intel = event_intel
        self.last_trade_time = None

    def select_strategy(self, metrics: AdvancedMetrics, spot: float) -> Tuple[str, List[Dict]]:
        """Select optimal strategy based on market regime"""
        now = datetime.now(IST)
        
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return "WAIT", []
        
        self.last_trade_time = now
        expiry = self._get_weekly_expiry()
        atm_strike = round(spot / 50) * 50 

        if metrics.regime in [MarketRegime.PANIC, MarketRegime.FEAR_BACKWARDATION, MarketRegime.DEFENSIVE_EVENT]:
            return self._defensive_strategies(atm_strike, expiry, metrics)
        elif metrics.regime in [MarketRegime.CALM_COMPRESSION, MarketRegime.LOW_VOL_COMPRESSION]:
            return self._premium_selling_strategies(atm_strike, expiry, metrics)
        elif metrics.regime == MarketRegime.BULL_EXPANSION:
            return self._bullish_strategies(atm_strike, expiry, metrics)
        else:
            return self._neutral_strategies(atm_strike, expiry, metrics)

    def _defensive_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Defensive strategies for high volatility"""
        if metrics.event_risk_score > 2.0:
            return (
                "DEFENSIVE_IRON_CONDOR",
                [
                    {"strike": atm + 600, "type": "CE", "side": "SELL", "expiry": expiry, "price": 10.0},
                    {"strike": atm + 800, "type": "CE", "side": "BUY", "expiry": expiry, "price": 5.0},
                    {"strike": atm - 600, "type": "PE", "side": "SELL", "expiry": expiry, "price": 10.0},
                    {"strike": atm - 800, "type": "PE", "side": "BUY", "expiry": expiry, "price": 5.0},
                ]
            )
        else:
            return (
                "DEFENSIVE_PUT_SPREAD",
                [
                    {"strike": atm - 200, "type": "PE", "side": "SELL", "expiry": expiry, "price": 50.0},
                    {"strike": atm - 400, "type": "PE", "side": "BUY", "expiry": expiry, "price": 20.0},
                ]
            )

    def _premium_selling_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Premium selling strategies for low volatility"""
        if metrics.ivp < 30:
            return (
                "SHORT_STRANGLE",
                [
                    {"strike": atm + 200, "type": "CE", "side": "SELL", "expiry": expiry, "price": 50.0},
                    {"strike": atm - 200, "type": "PE", "side": "SELL", "expiry": expiry, "price": 50.0},
                ]
            )
        else:
            return (
                "IRON_CONDOR",
                [
                    {"strike": atm + 300, "type": "CE", "side": "SELL", "expiry": expiry, "price": 40.0},
                    {"strike": atm + 500, "type": "CE", "side": "BUY", "expiry": expiry, "price": 10.0},
                    {"strike": atm - 300, "type": "PE", "side": "SELL", "expiry": expiry, "price": 40.0},
                    {"strike": atm - 500, "type": "PE", "side": "BUY", "expiry": expiry, "price": 10.0},
                ]
            )

    def _bullish_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Bullish strategies (Bull Put Spread)"""
        return (
            "BULL_PUT_SPREAD",
            [
                {"strike": atm - 100, "type": "PE", "side": "SELL", "expiry": expiry, "price": 70.0},
                {"strike": atm - 300, "type": "PE", "side": "BUY", "expiry": expiry, "price": 30.0},
            ]
        )

    def _neutral_strategies(self, atm: float, expiry: str, metrics: AdvancedMetrics) -> Tuple[str, List[Dict]]:
        """Neutral strategies"""
        return (
            "NEUTRAL_IRON_CONDOR",
            [
                {"strike": atm + 400, "type": "CE", "side": "SELL", "expiry": expiry, "price": 30.0},
                {"strike": atm + 600, "type": "CE", "side": "BUY", "expiry": expiry, "price": 5.0},
                {"strike": atm - 400, "type": "PE", "side": "SELL", "expiry": expiry, "price": 30.0},
                {"strike": atm - 600, "type": "PE", "side": "BUY", "expiry": expiry, "price": 5.0},
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
