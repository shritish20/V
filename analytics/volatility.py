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

    def get_volatility_metrics(self, current_vix: float) -> Tuple[float, float, float, float, float, float]:
        try:
            rv7 = self._calculate_realized_volatility(span=7)
            rv28 = self._calculate_realized_volatility(span=28)
            garch, egarch = self._calculate_garch_forecasts()
            ivp, iv_rank = self._calculate_iv_stats(current_vix)
            return rv7, rv28, garch, egarch, ivp, iv_rank
        except Exception as e:
            logger.error(f"Vol Metrics Failed: {e}")
            return 15.0, 15.0, 15.0, 15.0, 50.0, 50.0

    def calculate_volatility_regime(self, current_vix: float, iv_rank: float) -> str:
        if current_vix > 30.0 or iv_rank > 90.0: return "EXTREME_FEAR"
        elif iv_rank > 60.0: return "HIGH_VOL"
        elif iv_rank < 20.0 or current_vix < 12.0: return "LOW_VOL"
        return "NORMAL_VOL"

    def get_trend_status(self, spot: float) -> str:
        try:
            df = self.data_fetcher.nifty_data
            if df.empty or 'close' not in df.columns: return "NEUTRAL"
            
            window = min(20, len(df))
            if window < 5: return "NEUTRAL"
            
            ma20 = df['close'].tail(window).mean()
            if spot > ma20 * 1.01: return "BULL_TREND"
            if spot < ma20 * 0.99: return "BEAR_TREND"
            return "NEUTRAL"
        except: return "NEUTRAL"

    def _calculate_iv_stats(self, current_vix: float) -> Tuple[float, float]:
        try:
            # FIX: Check for column existence
            if self.data_fetcher.vix_data.empty or 'close' not in self.data_fetcher.vix_data.columns: 
                return 50.0, 50.0
            
            history = self.data_fetcher.vix_data['close'].dropna().values
            if len(history) < 10: return 50.0, 50.0
            
            # Dynamic Lookback (Fixes 248/252 error)
            lookback = min(252, len(history))
            relevant_history = history[-lookback:]
            
            ivp = float(percentileofscore(relevant_history, current_vix, kind='weak'))
            
            low, high = np.min(relevant_history), np.max(relevant_history)
            iv_rank = 50.0 if high == low else ((current_vix - low) / (high - low)) * 100.0
            
            return ivp, max(0.0, min(100.0, iv_rank))
        except Exception as e:
            logger.error(f"IV Stats Calc Error: {e}")
            return 50.0, 50.0

    def _calculate_realized_volatility(self, span=7) -> float:
        try:
            df = self.data_fetcher.nifty_data
            if df.empty or 'Log_Returns' not in df.columns: return 15.0
            
            returns = df['Log_Returns']
            if len(returns) < 5: return 15.0
            
            vol = returns.ewm(span=span).std().iloc[-1] * np.sqrt(252) * 100
            return float(vol) if not np.isnan(vol) else 15.0
        except: return 15.0

    def _calculate_garch_forecasts(self) -> Tuple[float, float]:
        try:
            df = self.data_fetcher.nifty_data
            if df.empty or 'Log_Returns' not in df.columns: return 15.0, 15.0

            cache_key = "garch_multi"
            if cache_key in self.vol_cache:
                val, ts = self.vol_cache[cache_key]
                if (datetime.now(IST) - ts).total_seconds() < 300: return val

            returns = df['Log_Returns'].dropna() * 100
            if len(returns) < 60: 
                fb = returns.std() * np.sqrt(252) if len(returns) > 5 else 15.0
                return fb, fb

            gm = arch_model(returns, vol='Garch', p=1, q=1, dist='t')
            res_g = gm.fit(disp='off')
            garch = np.sqrt(res_g.forecast(horizon=7).variance.iloc[-1].mean()) * np.sqrt(252)

            em = arch_model(returns, vol='EGARCH', p=1, q=1, dist='t')
            res_e = em.fit(disp='off')
            egarch = np.sqrt(res_e.forecast(horizon=1).variance.iloc[-1].iloc[0]) * np.sqrt(252)

            self.vol_cache[cache_key] = ((garch, egarch), datetime.now(IST))
            return garch, egarch
        except: return 15.0, 15.0
