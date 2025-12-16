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
        """
        Calculates the "Big 3" Vol Metrics: Realized, Forecast (GARCH), and Percentile.
        """
        try:
            realized_vol = self._calculate_realized_volatility()
            garch_vol = self._calculate_garch_forecast()
            iv_percentile = self.data_fetcher.calculate_iv_percentile(current_vix)
            return realized_vol, garch_vol, iv_percentile
        except Exception as e:
            logger.error(f"Vol metrics calculation failed: {e}")
            return 15.0, 15.0, 50.0

    def calculate_volatility_regime(self, current_vix: float, daily_return: float) -> str:
        """
        Z-SCORE REGIME CLASSIFIER
        """
        try:
            # 1. Fetch History
            history = self.data_fetcher.vix_data
            if history.empty or 'Close' not in history.columns:
                return "PANIC" if current_vix > 25 else "SAFE"

            # 2. Calculate Z-Score (20-Day Window)
            recent_vix = history['Close'].tail(20).values
            mean_vix = np.mean(recent_vix)
            std_vix = np.std(recent_vix)
            if std_vix < 0.1: std_vix = 0.1
            
            z_score = (current_vix - mean_vix) / std_vix
            
            # 3. Calculate Momentum
            ma5 = np.mean(recent_vix[-5:])
            is_accelerating = current_vix > ma5

            # --- REGIME DECISION TREE ---
            if z_score > 3.0:
                return "PANIC"
            elif z_score > 2.0:
                if daily_return < -0.015:
                    return "FEAR_BACKWARDATION"
                else:
                    return "VOL_EXPANSION"
            elif current_vix > 20 and not is_accelerating:
                return "VOL_COMPRESSION"
            elif z_score < -1.5:
                return "LOW_VOL_COMPRESSION"
            return "SAFE"

        except Exception as e:
            logger.error(f"Regime calculation failed: {e}")
            return "SAFE"

    def get_trend_status(self, spot: float) -> str:
        try:
            df = self.data_fetcher.nifty_data
            if df.empty or 'Close' not in df.columns: return "NEUTRAL"
            if len(df) < 20: return "NEUTRAL"
            
            ma20 = df['Close'].tail(20).mean()
            
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
            val, ts = self.vol_cache[cache_key]
            if (datetime.now(IST) - ts).total_seconds() < self.cache_ttl: return val

        try:
            if 'Log_Returns' not in self.data_fetcher.nifty_data.columns: return 15.0
            returns = self.data_fetcher.nifty_data['Log_Returns'].tail(window)
            rv = returns.std() * np.sqrt(252) * 100
            self.vol_cache[cache_key] = (rv, datetime.now(IST))
            return rv
        except: return 15.0

    def _calculate_garch_forecast(self, horizon: int = 1) -> float:
        cache_key = f"garch_{horizon}"
        if cache_key in self.vol_cache:
            val, ts = self.vol_cache[cache_key]
            if (datetime.now(IST) - ts).total_seconds() < self.cache_ttl: return val
            
        try:
            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna() * 100
            if len(returns) < 100: return 15.0
            model = arch_model(returns, vol='Garch', p=1, q=1)
            res = model.fit(disp='off')
            forecast = np.sqrt(res.forecast(horizon=horizon).variance.values[-1, -1]) * np.sqrt(252)
            self.vol_cache[cache_key] = (forecast, datetime.now(IST))
            return forecast
        except: return 15.0
