import logging
import math
from datetime import datetime
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
        """
        INSTITUTIONAL STRATEGY MATRIX v2.0
        """
        now = datetime.now(IST)
        bucket = CapitalBucket.WEEKLY
        
        # 1. Hard Filters
        available = capital_status.get("available", {}).get(bucket.value, 0)
        if available < 200000:
            return "WAIT", [], ExpiryType.WEEKLY, bucket
            
        if metrics.structure_confidence < 0.5:
            logger.warning(f"⛔ WAIT: Low Data Confidence ({metrics.structure_confidence:.2f})")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        if not metrics.expiry_date or metrics.expiry_date == "N/A":
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # 2. Market Context
        dte = metrics.days_to_expiry
        iv_rank = metrics.ivp
        is_event = metrics.regime == "BINARY_EVENT"
        
        # Determine Internal Regime
        if metrics.vix > 30 or iv_rank > 90:
            regime = "EXTREME_FEAR"
        elif metrics.vix > 18 or iv_rank > 60:
            regime = "HIGH_VOL"
        elif metrics.vix < 12 or iv_rank < 20:
            regime = "LOW_VOL"
        else:
            regime = "NORMAL"

        strategy = "WAIT"
        
        # 3. Selection Logic
        if is_event:
            logger.info(f"⚡ EVENT MODE: {metrics.top_event}. Selling Iron Fly.")
            strategy = "IRON_FLY"
            
        elif regime == "EXTREME_FEAR":
            # Crash Protection Logic: Fund the put via ratios
            logger.info("☢️ EXTREME FEAR: Put Ratio Spread (1x2).")
            strategy = "RATIO_SPREAD_PUT"
            
        elif regime == "HIGH_VOL":
            if dte <= 2:
                strategy = "SHORT_STRADDLE"
            else:
                strategy = "SHORT_STRANGLE"

        elif regime == "LOW_VOL":
            # Avoid Naked Selling. Use Zero-Upside-Risk strategies.
            logger.info("💤 LOW VOL: Jade Lizard Mode.")
            if metrics.trend_status == "BEAR_TREND":
                strategy = "REVERSE_JADE_LIZARD"
            else:
                strategy = "JADE_LIZARD"

        else: # NORMAL
            if metrics.trend_status == "BULL_TREND":
                strategy = "BULL_PUT_SPREAD"
            elif metrics.trend_status == "BEAR_TREND":
                strategy = "BEAR_CALL_SPREAD"
            else:
                strategy = "IRON_CONDOR"

        # 4. Leg Generation
        if strategy != "WAIT":
            legs = self._generate_pro_legs(strategy, spot, metrics.expiry_date, metrics)
            if legs:
                self.last_trade_time = now
                return strategy, legs, ExpiryType.WEEKLY, bucket

        return "WAIT", [], ExpiryType.WEEKLY, bucket

    def _generate_pro_legs(self, strategy: str, spot: float, expiry: str, metrics: AdvancedMetrics) -> List[Dict]:
        legs = []
        try:
            dte = max(1.0, metrics.days_to_expiry)
            iv = metrics.atm_iv if metrics.atm_iv > 0 else 15.0
            # Expected Move (1 SD)
            implied_move = spot * (iv / 100.0) * math.sqrt(dte / 365.0)
            
            def r50(price): return round(price / 50) * 50
            
            width = max(50, r50(implied_move))
            atm = r50(spot)

            if strategy == "RATIO_SPREAD_PUT":
                # Buy 1 ATM, Sell 2 OTM
                strike_long = r50(spot - (implied_move * 0.5))
                strike_short = r50(spot - (implied_move * 1.5))
                legs = [
                    {"strike": strike_long, "type": "PE", "side": "BUY", "expiry": expiry, "qty_mult": 1},
                    {"strike": strike_short, "type": "PE", "side": "SELL", "expiry": expiry, "qty_mult": 2}
                ]
            
            elif strategy == "JADE_LIZARD":
                # Sell OTM Put, Sell Call Spread
                put_strike = r50(spot - implied_move)
                call_short = r50(spot + implied_move)
                call_long = call_short + 50
                legs = [
                    {"strike": put_strike, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": call_short, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": call_long, "type": "CE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "IRON_FLY":
                legs = [
                    {"strike": atm, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": atm, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": atm + width, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": atm - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "SHORT_STRANGLE":
                call_strike = r50(spot + implied_move)
                put_strike = r50(spot - implied_move)
                legs = [
                    {"strike": call_strike, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": put_strike, "type": "PE", "side": "SELL", "expiry": expiry}
                ]

            elif strategy == "IRON_CONDOR":
                call_short = r50(spot + implied_move)
                put_short = r50(spot - implied_move)
                legs = [
                    {"strike": call_short, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": call_short + width, "type": "CE", "side": "BUY", "expiry": expiry},
                    {"strike": put_short, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": put_short - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]
            
            # Default fallbacks
            elif strategy == "BULL_PUT_SPREAD":
                sell_strike = r50(spot - (implied_move * 0.5))
                legs = [
                    {"strike": sell_strike, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": sell_strike - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

        except Exception as e:
            logger.error(f"Leg Generation Failed: {e}")
            return []

        return legs
