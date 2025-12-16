# File: trading/strategy_engine.py

import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import StrategyType, ExpiryType, CapitalBucket

logger = logging.getLogger("SmartStrategyEngine")

class IntelligentStrategyEngine:
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

        # 1. DEFENSIVE CHECKS
        if metrics.structure_confidence < 0.5:
            logger.warning(f"⛔ WAIT: Market Data Confidence Low ({metrics.structure_confidence:.2f})")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        if metrics.regime == "BINARY_EVENT":
            logger.warning(f"☢️ BINARY EVENT DETECTED ({metrics.top_event}). FREEZING.")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        available = capital_status.get("available", {}).get(bucket.value, 0)
        if available < 200000:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        expiry_date = metrics.expiry_date
        if not expiry_date or expiry_date == "N/A":
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # 2. MARKET DIAGNOSIS
        dte = metrics.days_to_expiry
        vrp = metrics.vrp_score
        
        is_panic = metrics.regime in ["PANIC", "FEAR_BACKWARDATION"] or metrics.vix > 24.0
        is_low_vol = metrics.vix < 13.0 or metrics.ivp < 20
        is_backwardation = metrics.term_structure_slope < -1.0 
        is_expensive = vrp > 3.0
        is_cheap = vrp < 0.0
        trend = metrics.trend_status
        strategy = "WAIT"

        # 3. STRATEGY SELECTION MATRIX
        if is_panic or is_backwardation:
            logger.info("🚨 REGIME: PANIC/BACKWARDATION. Deploying 1x2 Ratio Spreads.")
            strategy = "RATIO_SPREAD_PUT"

        elif dte <= 2.0:
            if is_expensive:
                logger.info("⚡ REGIME: EXPIRY WEEK (High Vol). Deploying Long Calendars.")
                strategy = "LONG_CALENDAR_PUT"
                bucket = CapitalBucket.MONTHLY
            else:
                strategy = "IRON_CONDOR"

        elif is_low_vol or is_cheap:
            logger.info("💤 REGIME: LOW VOL. Buying Calendars.")
            if trend == "BULLISH_TREND":
                strategy = "LONG_CALENDAR_CALL"
            else:
                strategy = "LONG_CALENDAR_PUT"
            bucket = CapitalBucket.MONTHLY

        elif trend == "BULLISH_TREND" and is_expensive:
            logger.info("📈 REGIME: BULL TREND. Deploying Jade Lizard.")
            strategy = "JADE_LIZARD"

        elif is_expensive: 
            strategy = "IRON_CONDOR"
        
        else:
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        if strategy != "WAIT":
            legs = self._generate_pro_legs(strategy, spot, expiry_date, metrics)
            if legs:
                self.last_trade_time = now
                return strategy, legs, ExpiryType.WEEKLY, bucket
        
        return "WAIT", [], ExpiryType.WEEKLY, bucket

    def _generate_pro_legs(self, strategy: str, spot: float, expiry: str, metrics: AdvancedMetrics) -> List[Dict]:
        legs = []
        try:
            dte = max(1.0, metrics.days_to_expiry)
            iv = metrics.atm_iv if metrics.atm_iv > 0 else 15.0
            implied_move = spot * (iv / 100.0) * math.sqrt(dte / 365.0)
            wing_width = max(100, round((implied_move * 0.5) / 50) * 50)
            def r50(price): return round(price / 50) * 50

            if strategy == "RATIO_SPREAD_PUT":
                atm_pe = r50(spot)
                otm_pe = r50(spot - (implied_move * 1.2))
                legs = [
                    {"strike": atm_pe, "type": "PE", "side": "BUY", "expiry": expiry, "qty_mult": 1},
                    {"strike": otm_pe, "type": "PE", "side": "SELL", "expiry": expiry, "qty_mult": 2} 
                ]

            elif strategy == "JADE_LIZARD":
                sell_put = r50(spot - implied_move)
                sell_call = r50(spot + implied_move)
                buy_call = r50(spot + implied_move + wing_width)
                legs = [
                    {"strike": sell_put, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": sell_call, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": buy_call, "type": "CE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "LONG_CALENDAR_PUT":
                far_expiry = self._get_far_expiry(expiry)
                if not far_expiry: return []
                atm = r50(spot)
                legs = [
                    {"strike": atm, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm, "type": "PE", "side": "BUY", "expiry": far_expiry}
                ]
            
            elif strategy == "LONG_CALENDAR_CALL":
                far_expiry = self._get_far_expiry(expiry)
                if not far_expiry: return []
                atm = r50(spot)
                legs = [
                    {"strike": atm, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm, "type": "CE", "side": "BUY", "expiry": far_expiry}
                ]

            elif strategy == "IRON_CONDOR":
                call_short = r50(spot + implied_move)
                put_short = r50(spot - implied_move)
                legs = [
                    {"strike": call_short, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": call_short + wing_width, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": put_short, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": put_short - wing_width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

        except Exception as e:
            logger.error(f"Strategy Leg Generation Failed: {e}")
            return []

        return legs

    def _get_far_expiry(self, near_expiry_str: str) -> Optional[str]:
        if not self.instruments_master: return None
        try:
            near_dt = datetime.strptime(near_expiry_str, "%Y-%m-%d").date()
            all_exp = self.instruments_master.get_all_expiries("NIFTY")
            for e in all_exp:
                days = (e - near_dt).days
                if 25 <= days <= 50: return e.strftime("%Y-%m-%d")
            for e in all_exp:
                days = (e - near_dt).days
                if 7 < days <= 60: return e.strftime("%Y-%m-%d")
        except Exception:
            pass
        return None
