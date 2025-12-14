# File: analytics/volatility.py

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict, List
import logging
from arch import arch_model
from core.config import settings, IST
from utils.data_fetcher import DashboardDataFetcher

logger = logging.getLogger("VolAnalytics")

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
            return realized_vol, garch_vol, iv_percentile
        except Exception as e:
            logger.error(f"Vol metrics calculation failed: {e}")
            return 15.0, 15.0, 50.0

    def get_trend_status(self, spot: float) -> str:
        """
        INSTITUTIONAL TREND FILTER
        Uses 20-Day Moving Average from nifty_50.csv.
        Includes 1% buffer to avoid whipsaws.
        """
        try:
            df = self.data_fetcher.nifty_data
            if df.empty or 'Close' not in df.columns:
                return "NEUTRAL"
            
            # Ensure enough data
            if len(df) < 20:
                return "NEUTRAL"
            
            # Calculate MA20 (Data is sorted Oldest -> Newest)
            ma20 = df['Close'].tail(20).mean()
            
            # Logic: Price > MA20 = Bullish
            # Added 1% Buffer to avoid noise
            if spot > ma20 * 1.01:
                return "BULLISH_TREND"
            elif spot < ma20 * 0.99:
                return "BEARISH_TREND"
            else:
                return "NEUTRAL"
                
        except Exception as e:
            logger.error(f"Trend calculation failed: {e}")
            return "NEUTRAL"

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
            if len(returns) < 100: return 15.0
            
            model = arch_model(returns, vol='Garch', p=1, q=1)
            fitted_model = model.fit(disp='off')
            forecast = fitted_model.forecast(horizon=horizon)
            
            garch_vol_daily = np.sqrt(forecast.variance.values[-1, -1])
            garch_vol_annual = garch_vol_daily * np.sqrt(252)
            
            self.vol_cache[cache_key] = (garch_vol_annual, datetime.now(IST))
            return garch_vol_annual
        except Exception as e:
            logger.error(f"GARCH forecast failed: {e}")
            return 15.0

    def calculate_volatility_regime(self, vix, ivp, realized_vol, daily_return, event_score) -> str:
        # Standard Logic
        iv_rv_spread = vix - realized_vol
        if vix > 25 and (daily_return < -0.015 or iv_rv_spread > 6.0): return "PANIC"
        if event_score >= 3.0: return "BINARY_EVENT"
        
        if vix > 18 or ivp > 65:
            if daily_return > 0.005: return "BULL_EXPANSION"
            elif daily_return < -0.005: return "FEAR_BACKWARDATION"
            return "TRANSITION"
            
        if ivp < 30:
            if daily_return < -0.005: return "BEAR_CONTRACTION"
            return "LOW_VOL_COMPRESSION"
            
        return "CALM_COMPRESSION"

    def calculate_volatility_surface(self, chain_data, spot):
        # Implementation preserved
        surface_points = []
        try:
            for item in chain_data:
                strike = item.get('strike_price', 0)
                ce_iv = item.get('call_options', {}).get('option_greeks', {}).get('iv', 0)
                pe_iv = item.get('put_options', {}).get('option_greeks', {}).get('iv', 0)
                if ce_iv == 0 or pe_iv == 0: continue
                
                moneyness = ((strike - spot) / spot) * 100
                surface_points.append({
                    'strike': strike, 'moneyness': moneyness,
                    'call_iv': ce_iv, 'put_iv': pe_iv, 'iv_skew': pe_iv - ce_iv
                })
            return surface_points
        except: return []

    def calculate_chain_metrics(self, chain_data):
        return {"pcr": 1.0, "straddle_price": 0.0, "max_pain": 0.0}
