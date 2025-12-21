import numpy as np
import pandas as pd
import logging
from datetime import datetime
from typing import Tuple, Dict
from arch import arch_model
from scipy.stats import percentileofscore
from core.config import settings, IST

logger = logging.getLogger("VolAnalytics")

class HybridVolatilityAnalytics:
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        self.vol_cache: Dict[str, Tuple[float, datetime]] = {}

    def get_volatility_metrics(self, current_vix: float) -> Tuple[float, float, float, float, float]:
        """
        Returns: RV_7D, RV_28D, GARCH, EGARCH, IVRank
        """
        try:
            rv7 = self._calculate_realized_volatility(span=7)
            rv28 = self._calculate_realized_volatility(span=28)
            garch, egarch = self._calculate_garch_forecasts()
            iv_rank = self._calculate_iv_rank(current_vix)
            return rv7, rv28, garch, egarch, iv_rank
        except Exception as e:
            logger.error(f"Vol Metrics Failed: {e}")
            return 15.0, 15.0, 15.0, 15.0, 50.0

    def calculate_volatility_regime(self, current_vix: float, iv_rank: float) -> str:
        if current_vix > 30.0 or iv_rank > 90.0:
            return "EXTREME_FEAR"
        elif iv_rank > 60.0:
            return "HIGH_VOL"
        elif iv_rank < 20.0 or current_vix < 12.0:
            return "LOW_VOL"
        return "NORMAL_VOL"

    def get_trend_status(self, spot: float) -> str:
        try:
            df = self.data_fetcher.nifty_data
            if df.empty or 'close' not in df.columns: return "NEUTRAL"
            
            ma20 = df['close'].tail(20).mean()
            if spot > ma20 * 1.01: return "BULL_TREND"
            if spot < ma20 * 0.99: return "BEAR_TREND"
            return "NEUTRAL"
        except: return "NEUTRAL"

    def _calculate_iv_rank(self, current_vix: float) -> float:
        try:
            if self.data_fetcher.vix_data.empty: return 50.0
            history = self.data_fetcher.vix_data['close'].tail(252).values
            if len(history) < 10: return 50.0
            return float(percentileofscore(history, current_vix, kind='weak'))
        except: return 50.0

    def _calculate_realized_volatility(self, span=7) -> float:
        try:
            if 'Log_Returns' not in self.data_fetcher.nifty_data.columns: return 15.0
            # Use EWMA as per user script
            returns = self.data_fetcher.nifty_data['Log_Returns']
            vol = returns.ewm(span=span).std().iloc[-1] * np.sqrt(252) * 100
            return float(vol)
        except: return 15.0

    def _calculate_garch_forecasts(self) -> Tuple[float, float]:
        """Returns GARCH_7D and EGARCH_1D"""
        try:
            cache_key = "garch_multi"
            if cache_key in self.vol_cache:
                val, ts = self.vol_cache[cache_key] # val is tuple (garch, egarch)
                if (datetime.now(IST) - ts).total_seconds() < 300: return val

            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna() * 100
            if len(returns) < 126: return 15.0, 15.0

            # 1. GARCH(1,1) - 7 Day Forecast
            gm = arch_model(returns, vol='Garch', p=1, q=1, dist='t')
            res_g = gm.fit(disp='off')
            garch_7d = np.sqrt(res_g.forecast(horizon=7).variance.iloc[-1].mean()) * np.sqrt(252)

            # 2. EGARCH(1,1) - 1 Day Forecast (Crash Watch)
            em = arch_model(returns, vol='EGARCH', p=1, q=1, dist='t')
            res_e = em.fit(disp='off')
            egarch_1d = np.sqrt(res_e.forecast(horizon=1).variance.iloc[-1].iloc[0]) * np.sqrt(252)

            self.vol_cache[cache_key] = ((garch_7d, egarch_1d), datetime.now(IST))
            return garch_7d, egarch_1d
        except Exception as e: 
            return 15.0, 15.0
