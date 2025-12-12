import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict, List
import logging
from arch import arch_model
from core.config import settings, IST
from utils.data_fetcher import DashboardDataFetcher

logger = logging.getLogger("VolGuard18")

class HybridVolatilityAnalytics:
    def __init__(self):
        self.data_fetcher = DashboardDataFetcher()
        self.vol_cache: Dict[str, Tuple[float, datetime]] = {}
        self.cache_ttl = 300

    def get_volatility_metrics(self, current_vix: float) -> Tuple[float, float, float]:
        try:
            realized_vol = self._calculate_realized_volatility()
            garch_vol = self._calculate_garch_forecast()
            iv_percentile = self.data_fetcher.calculate_iv_percentile(current_vix)

            logger.debug(f"Vol Metrics | Realized: {realized_vol:.2f}% | GARCH: {garch_vol:.2f}% | IVP: {iv_percentile:.2f}%")
            return realized_vol, garch_vol, iv_percentile
        except Exception as e:
            logger.error(f"Volatility metrics calculation failed: {e}")
            return 15.0, 15.0, 50.0

    def _calculate_realized_volatility(self, window: int = 7) -> float:
        cache_key = f"realized_vol_{window}"
        if cache_key in self.vol_cache:
            value, timestamp = self.vol_cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return value

        try:
            if 'Log_Returns' not in self.data_fetcher.nifty_data.columns:
                return 15.0
            
            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna().tail(window)
            if returns.empty: return 15.0
            
            realized_vol = returns.std() * np.sqrt(252) * 100
            self.vol_cache[cache_key] = (realized_vol, datetime.now(IST))
            return realized_vol
        except Exception as e:
            logger.error(f"Realized vol calculation failed: {e}")
            return 15.0

    def _calculate_garch_forecast(self, horizon: int = 1) -> float:
        cache_key = f"garch_forecast_{horizon}"
        if cache_key in self.vol_cache:
            value, timestamp = self.vol_cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return value

        try:
            if 'Log_Returns' not in self.data_fetcher.nifty_data.columns:
                return 15.0

            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna().tail(252) * 100
            if len(returns) < 100:
                return 15.0

            model = arch_model(returns, vol='Garch', p=1, q=1)
            fitted_model = model.fit(disp='off')
            forecast = fitted_model.forecast(horizon=horizon)
            
            # Forecast is variance, take sqrt to get vol
            garch_vol_daily = np.sqrt(forecast.variance.values[-1, -1])
            garch_vol_annual = garch_vol_daily * np.sqrt(252)

            self.vol_cache[cache_key] = (garch_vol_annual, datetime.now(IST))
            return garch_vol_annual
        except Exception as e:
            logger.error(f"GARCH forecast failed: {e}")
            return 15.0

    def calculate_volatility_regime(
        self, 
        vix: float, 
        ivp: float, 
        realized_vol: float, 
        daily_return: float, 
        event_score: float
    ) -> str:
        """
        Endgame Regime Detection v2.0
        Integrates Trend (Returns), Cost (IVP), and Risk (Events).
        """
        # 1. Spread: Implied vs Realized
        iv_rv_spread = vix - realized_vol

        # 2. PRIORITY 1: PANIC (Crash Mode)
        # VIX > 25 AND (Market Crashing OR Spread Huge)
        if vix > 25 and (daily_return < -0.015 or iv_rv_spread > 6.0):
            return "PANIC"

        # 3. PRIORITY 2: EVENT RISK (Binary Event Pending)
        if event_score >= 3.0:
            return "DEFENSIVE_EVENT"

        # 4. PRIORITY 3: FEAR vs. BULL EXPANSION
        if vix > 18 or ivp > 65:
            # High Vol + Market up = Bullish Speculation
            if daily_return > 0.005:
                return "BULL_EXPANSION"
            # High Vol + Market Down = Fear
            elif daily_return < -0.005:
                return "FEAR_BACKWARDATION"
            else:
                return "TRANSITION"

        # 5. PRIORITY 4: COMPRESSION (Low Vol)
        if ivp < 30:
            if daily_return < -0.005:
                return "BEAR_CONTRACTION"
            return "LOW_VOL_COMPRESSION"

        return "CALM_COMPRESSION"

    def calculate_volatility_surface(self, chain_data: List[Dict], spot: float) -> List[Dict]:
        """Calculates Skew from Option Chain"""
        surface_points = []
        try:
            for item in chain_data:
                strike = item.get('strike_price', 0)
                ce_data = item.get('call_options', {})
                pe_data = item.get('put_options', {})

                if not ce_data or not pe_data: continue

                ce_iv = ce_data.get('option_greeks', {}).get('iv', 0)
                pe_iv = pe_data.get('option_greeks', {}).get('iv', 0)

                if ce_iv == 0 or pe_iv == 0: continue

                moneyness = ((strike - spot) / spot) * 100

                surface_points.append({
                    'strike': strike,
                    'moneyness': moneyness,
                    'call_iv': ce_iv,
                    'put_iv': pe_iv,
                    'iv_skew': ce_iv - pe_iv, # <- SKEW CALCULATION
                })
            return surface_points
        except Exception as e:
            logger.error(f"Volatility surface calculation failed: {e}")
            return []

    def calculate_chain_metrics(self, chain_data: List[Dict]) -> Dict:
        """
        Helper to get aggregate chain metrics (PCR, Straddle Price).
        Calculates Straddle Price by finding the ATM strike where 
        Call Price is closest to Put Price (Min Delta).
        """
        call_oi = 0
        put_oi = 0
        
        # Straddle Price Discovery
        min_diff = float('inf')
        straddle_price = 0.0

        try:
            for item in chain_data:
                ce = item.get('call_options', {}).get('market_data', {})
                pe = item.get('put_options', {}).get('market_data', {})
                
                # 1. Sum OI
                call_oi += ce.get('oi', 0)
                put_oi += pe.get('oi', 0)

                # 2. Find Straddle Price (ATM Proxy)
                # We identify ATM as the strike where |Call_LTP - Put_LTP| is minimized.
                ce_ltp = ce.get('ltp', 0)
                pe_ltp = pe.get('ltp', 0)
                
                if ce_ltp > 0 and pe_ltp > 0:
                    diff = abs(ce_ltp - pe_ltp)
                    if diff < min_diff:
                        min_diff = diff
                        straddle_price = ce_ltp + pe_ltp

            pcr = round(put_oi / call_oi, 2) if call_oi > 0 else 1.0
            
            # If logic failed (e.g. bad data), return 0 so engine falls back to 1% rule
            if straddle_price == 0:
                logger.debug("Straddle price calc found no valid data, returning 0")

            return {
                "call_oi": call_oi, 
                "put_oi": put_oi, 
                "pcr": pcr,
                "straddle_price": straddle_price
            }

        except Exception as e:
            logger.error(f"Chain metrics calculation failed: {e}")
            return {"pcr": 1.0, "straddle_price": 0.0}
