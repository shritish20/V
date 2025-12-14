# File: trading/strategy_engine.py

import logging
import math
from datetime import datetime
from typing import List, Dict, Tuple
from core.config import settings, IST
from core.models import AdvancedMetrics
from core.enums import StrategyType, ExpiryType, CapitalBucket

logger = logging.getLogger("StrategyEngine")

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

        # 1. Data Integrity Check (CRITICAL FIX)
        if metrics.structure_confidence < 0.5:
            logger.warning("⛔ WAIT: Market Data Integrity Low")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # 2. VRP Sanity Check (CRITICAL FIX)
        if metrics.vrp_score > 15.0:
            logger.critical(f"⛔ WAIT: VRP {metrics.vrp_score:.1f} too high (Model Mismatch)")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # 3. Standard Checks
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 300:
            return "WAIT", [], ExpiryType.WEEKLY, bucket
        
        available = capital_status.get("available", {}).get(bucket.value, 0)
        if available < 250000: return "WAIT", [], ExpiryType.WEEKLY, bucket

        expiry_date = metrics.expiry_date
        if not expiry_date or expiry_date == "N/A": 
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # --- LOGIC MATRIX ---
        if metrics.regime == "BINARY_EVENT":
            logger.warning(f"☢️ BINARY EVENT ({metrics.top_event}).")
            if metrics.vrp_score < 0:
                return "LONG_STRADDLE", self._generate_straddle(spot, expiry_date, "BUY"), ExpiryType.WEEKLY, bucket
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        dte = metrics.days_to_expiry
        is_expiry_week = dte <= 2.0
        vrp = metrics.vrp_score
        is_rich = vrp > 4.0
        is_cheap = vrp < 0.0
        is_backwardation = metrics.term_structure_slope < -2.0
        is_contango = metrics.term_structure_slope > 1.0
        trend = metrics.trend_status

        strategy = "WAIT"

        # --- SCENARIO 1: EXPIRY WEEK (Gamma Defense) ---
        if is_expiry_week:
            if is_rich: strategy = "IRON_FLY"
            elif is_cheap: 
                strategy = "LONG_CALENDAR_CALL" if trend == "BULLISH_TREND" else "LONG_CALENDAR_PUT"
                bucket = CapitalBucket.MONTHLY
        else: 
            if is_rich and is_backwardation: strategy = "SHORT_STRANGLE"
            elif trend == "BULLISH_TREND" and metrics.volatility_skew > 4.0:
                strategy = "RATIO_SPREAD_PUT"
            elif is_contango and (is_rich or 0 < vrp <= 4.0):
                strategy = "IRON_CONDOR"
            elif is_cheap:
                strategy = "LONG_CALENDAR_CALL" if trend == "BULLISH_TREND" else "LONG_CALENDAR_PUT"
                bucket = CapitalBucket.MONTHLY

        if strategy != "WAIT":
            legs = self._generate_pro_legs(strategy, spot, expiry_date, metrics)
            if legs:
                self.last_trade_time = now
                return strategy, legs, ExpiryType.WEEKLY, bucket

        return "WAIT", [], ExpiryType.WEEKLY, bucket

    def _generate_pro_legs(self, strategy, spot, expiry, metrics) -> List[Dict]:
        legs = []
        try:
            dte = max(1.0, metrics.days_to_expiry)
            iv = metrics.atm_iv if metrics.atm_iv > 0 else 15.0
            implied_move = spot * (iv / 100.0) * math.sqrt(dte / 365.0)
            width = max(100, round(implied_move / 50) * 50)
            strangle_width = max(150, round((implied_move * 1.5) / 50) * 50)

            if strategy == "IRON_FLY":
                atm = self._round_strike(spot)
                legs = [
                    {"strike": atm, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm + width, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": atm - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "IRON_CONDOR":
                ce_sell = self._round_strike(spot + implied_move)
                pe_sell = self._round_strike(spot - implied_move)
                legs = [
                    {"strike": ce_sell, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": ce_sell + width, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": pe_sell, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": pe_sell - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "SHORT_STRANGLE":
                ce_sell = self._round_strike(spot + strangle_width)
                pe_sell = self._round_strike(spot - strangle_width)
                legs = [
                    {"strike": ce_sell, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": pe_sell, "type": "PE", "side": "SELL", "expiry": expiry}
                ]

            # BROKEN WING BUTTERFLY (Ratio Spread Fix)
            elif strategy == "RATIO_SPREAD_PUT":
                atm_pe = self._round_strike(spot)
                otm_pe = self._round_strike(spot - implied_move)
                
                # Capped Wing Distance (Max 1500pts)
                wing_dist = min(1500, implied_move * 2.5)
                disaster_wing = self._round_strike(spot - wing_dist)
                
                legs = [
                    {"strike": atm_pe, "type": "PE", "side": "BUY", "expiry": expiry},
                    {"strike": otm_pe, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": otm_pe, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": disaster_wing, "type": "PE", "side": "BUY", "expiry": expiry} # SAFETY CAP
                ]

            elif "CALENDAR" in strategy:
                far_expiry = self._get_far_expiry(expiry)
                if not far_expiry: return []
                atm = self._round_strike(spot)
                otype = "CE" if "CALL" in strategy else "PE"
                legs = [
                    {"strike": atm, "type": otype, "side": "SELL", "expiry": expiry},
                    {"strike": atm, "type": otype, "side": "BUY", "expiry": far_expiry}
                ]
            
            elif strategy == "LONG_STRADDLE":
                atm = self._round_strike(spot)
                legs = [
                    {"strike": atm, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": atm, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

        except Exception as e:
            logger.error(f"Leg Gen Error: {e}")
            return []
            
        return legs

    def _round_strike(self, price):
        return round(price / 50) * 50

    def _get_far_expiry(self, near_expiry_str):
        if not self.instruments_master: return None
        all_exp = self.instruments_master.get_all_expiries("NIFTY")
        if not all_exp: return None
        
        near_dt = datetime.strptime(near_expiry_str, "%Y-%m-%d").date()
        for e in all_exp:
            days = (e - near_dt).days
            if 25 <= days <= 45: return e.strftime("%Y-%m-%d")
        
        for e in all_exp:
            days = (e - near_dt).days
            if 7 < days <= 60: return e.strftime("%Y-%m-%d")
                
        return None

    def _generate_straddle(self, spot, expiry, side="BUY"):
        atm = self._round_strike(spot)
        return [
            {"strike": atm, "type": "CE", "side": side, "expiry": expiry},
            {"strike": atm, "type": "PE", "side": side, "expiry": expiry}
        ]
