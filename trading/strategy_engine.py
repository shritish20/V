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
        self.cooldown_seconds = 300  # 5 Minute mandatory cooldown to prevent machine-gun ordering

    def set_instruments_master(self, master):
        self.instruments_master = master

    def select_strategy_with_capital(self, metrics: AdvancedMetrics, spot: float, 
                                   capital_status: Dict) -> Tuple[str, List[Dict], ExpiryType, CapitalBucket]:
        """
        INSTITUTIONAL STRATEGY MATRIX v2.2 (Strict Time & Spam Gates)
        """
        now = datetime.now(IST)
        bucket = CapitalBucket.WEEKLY
        
        # ------------------------------------------------------------------
        # 1. MARKET HOURS CHECK (The "Care" Factor)
        # ------------------------------------------------------------------
        # We strictly strictly enforce trading only between 09:15 and 15:15
        # This prevents "Night Trading" or "Post-Close" ghost orders.
        if not (settings.MARKET_OPEN_TIME <= now.time() <= settings.SAFE_TRADE_END):
            # Only log occasionally to avoid cluttering logs
            if now.second < 10 and now.minute % 5 == 0: 
                logger.info(f"💤 Market Closed (Time: {now.strftime('%H:%M')}). Strategy Sleeping.")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # ------------------------------------------------------------------
        # 2. SPAM PROTECTION (Cooldown Timer)
        # ------------------------------------------------------------------
        if self.last_trade_time:
            time_since_trade = (now - self.last_trade_time).total_seconds()
            if time_since_trade < self.cooldown_seconds:
                # Silently wait. Do not log "WAIT" to keep logs clean.
                return "WAIT", [], ExpiryType.WEEKLY, bucket

        # ------------------------------------------------------------------
        # 3. CAPITAL & DATA INTEGRITY
        # ------------------------------------------------------------------
        available = capital_status.get("available", {}).get(bucket.value, 0)
        # Minimum capital buffer (2 Lakhs) - Hard Stop
        if available < 200000:
            return "WAIT", [], ExpiryType.WEEKLY, bucket
            
        if metrics.structure_confidence < 0.5:
            # Only warn if we haven't warned recently
            if not self.last_trade_time or (now - self.last_trade_time).total_seconds() > 600:
                logger.warning(f"⛔ WAIT: Low Data Confidence ({metrics.structure_confidence:.2f})")
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        if not metrics.expiry_date or metrics.expiry_date == "N/A":
            return "WAIT", [], ExpiryType.WEEKLY, bucket

        # ------------------------------------------------------------------
        # 4. MARKET REGIME DIAGNOSIS
        # ------------------------------------------------------------------
        dte = metrics.days_to_expiry
        iv_rank = metrics.ivp
        is_event = metrics.regime == "BINARY_EVENT"
        
        if metrics.vix > 30 or iv_rank > 90:
            regime = "EXTREME_FEAR"
        elif metrics.vix > 18 or iv_rank > 60:
            regime = "HIGH_VOL"
        elif metrics.vix < 12 or iv_rank < 20:
            regime = "LOW_VOL"
        else:
            regime = "NORMAL"

        strategy = "WAIT"
        
        # ------------------------------------------------------------------
        # 5. STRATEGY SELECTION
        # ------------------------------------------------------------------
        if is_event:
            logger.info(f"⚡ EVENT MODE: {metrics.top_event}. Selling Iron Fly.")
            strategy = "IRON_FLY"
            
        elif regime == "EXTREME_FEAR":
            # Crash Defense: 1x2 Ratio Spread to finance protection
            logger.info("☢️ EXTREME FEAR: Put Ratio Spread (1x2).")
            strategy = "RATIO_SPREAD_PUT"
            
        elif regime == "HIGH_VOL":
            if dte <= 2:
                strategy = "SHORT_STRADDLE"
            else:
                strategy = "SHORT_STRANGLE"

        elif regime == "LOW_VOL":
            # Smart Skew: Jade Lizard (Zero Upside Risk)
            # This is better than Iron Condor in low vol because call premiums are trash
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

        # ------------------------------------------------------------------
        # 6. LEG GENERATION
        # ------------------------------------------------------------------
        if strategy != "WAIT":
            legs = self._generate_pro_legs(strategy, spot, metrics.expiry_date, metrics)
            if legs:
                # Update last trade time ONLY if we successfully generate a trade
                self.last_trade_time = now
                logger.info(f"✨ Signal Generated: {strategy} (Regime: {regime}, VIX: {metrics.vix:.1f})")
                return strategy, legs, ExpiryType.WEEKLY, bucket

        return "WAIT", [], ExpiryType.WEEKLY, bucket

    def _generate_pro_legs(self, strategy: str, spot: float, expiry: str, metrics: AdvancedMetrics) -> List[Dict]:
        legs = []
        try:
            dte = max(1.0, metrics.days_to_expiry)
            iv = metrics.atm_iv if metrics.atm_iv > 0 else 15.0
            # Expected Move (1 Standard Deviation)
            implied_move = spot * (iv / 100.0) * math.sqrt(dte / 365.0)
            
            # Round to nearest 50 for NIFTY
            def r50(price): return round(price / 50) * 50
            
            width = max(50, r50(implied_move))
            
            # --- STRATEGY BUILDER ---

            if strategy == "RATIO_SPREAD_PUT":
                # Buy 1 ATM Put, Sell 2 OTM Puts
                strike_long = r50(spot - (implied_move * 0.5))
                strike_short = r50(spot - (implied_move * 1.5))
                legs = [
                    {"strike": strike_long, "type": "PE", "side": "BUY", "expiry": expiry, "qty_mult": 1},
                    {"strike": strike_short, "type": "PE", "side": "SELL", "expiry": expiry, "qty_mult": 2}
                ]
            
            elif strategy == "JADE_LIZARD":
                # Sell OTM Put, Sell OTM Call Spread
                # Goal: Net Credit > Call Spread Width (Zero Risk Upside)
                put_strike = r50(spot - implied_move)
                call_short = r50(spot + implied_move)
                call_long = call_short + 100 # Wider wing to ensure credit covers width
                
                legs = [
                    {"strike": put_strike, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": call_short, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": call_long, "type": "CE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "REVERSE_JADE_LIZARD":
                call_strike = r50(spot + implied_move)
                put_short = r50(spot - implied_move)
                put_long = put_short - 100
                legs = [
                    {"strike": call_strike, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": put_short, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": put_long, "type": "PE", "side": "BUY", "expiry": expiry}
                ]

            elif strategy == "IRON_FLY":
                atm = r50(spot)
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
            
            elif strategy == "BULL_PUT_SPREAD":
                sell_strike = r50(spot - (implied_move * 0.5))
                legs = [
                    {"strike": sell_strike, "type": "PE", "side": "SELL", "expiry": expiry},
                    {"strike": sell_strike - width, "type": "PE", "side": "BUY", "expiry": expiry}
                ]
                
            elif strategy == "BEAR_CALL_SPREAD":
                sell_strike = r50(spot + (implied_move * 0.5))
                legs = [
                    {"strike": sell_strike, "type": "CE", "side": "SELL", "expiry": expiry},
                    {"strike": sell_strike + width, "type": "CE", "side": "BUY", "expiry": expiry}
                ]

        except Exception as e:
            logger.error(f"Leg Generation Failed: {e}")
            return []

        return legs
