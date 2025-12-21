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
        """
        Returns: RV_7D, RV_28D, GARCH, EGARCH, IVP, IV_Rank
        """
        try:
            # 1. Realized Volatility
            rv7 = self._calculate_realized_volatility(span=7)
            rv28 = self._calculate_realized_volatility(span=28)
            
            # 2. GARCH Models
            garch, egarch = self._calculate_garch_forecasts()
            
            # 3. IV Rank & Percentile (Dynamic Lookback)
            ivp, iv_rank = self._calculate_iv_stats(current_vix)
            
            return rv7, rv28, garch, egarch, ivp, iv_rank
        except Exception as e:
            logger.error(f"Vol Metrics Failed: {e}")
            # Return safe defaults if everything explodes
            return 15.0, 15.0, 15.0, 15.0, 50.0, 50.0

    def calculate_volatility_regime(self, current_vix: float, iv_rank: float) -> str:
        # Use IV Rank for regime as it's cleaner for relative levels
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
            
            # Dynamic lookback for MA
            available_days = len(df)
            window = min(20, available_days)
            if window < 5: return "NEUTRAL"

            ma20 = df['close'].tail(window).mean()
            if spot > ma20 * 1.01: return "BULL_TREND"
            if spot < ma20 * 0.99: return "BEAR_TREND"
            return "NEUTRAL"
        except: return "NEUTRAL"

    def _calculate_iv_stats(self, current_vix: float) -> Tuple[float, float]:
        """
        Calculates BOTH IV Percentile and IV Rank dynamically.
        Does not crash if < 252 days.
        """
        try:
            if self.data_fetcher.vix_data.empty: 
                return 50.0, 50.0
            
            # Get available history (up to 1 year, but adapt if less)
            history = self.data_fetcher.vix_data['close'].dropna().values
            total_days = len(history)
            
            if total_days < 10: 
                # Not enough data for meaningful stats
                return 50.0, 50.0
            
            # Use max 252 days, or whatever we have
            lookback = min(252, total_days)
            relevant_history = history[-lookback:]
            
            # 1. IV Percentile (Frequency)
            # "What % of days was VIX lower than today?"
            ivp = float(percentileofscore(relevant_history, current_vix, kind='weak'))
            
            # 2. IV Rank (Relative Range)
            # "Where is today between the Year High and Year Low?"
            low = np.min(relevant_history)
            high = np.max(relevant_history)
            
            if high == low:
                iv_rank = 50.0
            else:
                iv_rank = ((current_vix - low) / (high - low)) * 100.0
                
            # Clamp to 0-100 just in case
            iv_rank = max(0.0, min(100.0, iv_rank))
            
            return ivp, iv_rank
            
        except Exception as e:
            logger.error(f"IV Stats Calc Error: {e}")
            return 50.0, 50.0

    def _calculate_realized_volatility(self, span=7) -> float:
        try:
            if 'Log_Returns' not in self.data_fetcher.nifty_data.columns: return 15.0
            returns = self.data_fetcher.nifty_data['Log_Returns']
            if len(returns) < 5: return 15.0
            
            vol = returns.ewm(span=span).std().iloc[-1] * np.sqrt(252) * 100
            return float(vol) if not np.isnan(vol) else 15.0
        except: return 15.0

    def _calculate_garch_forecasts(self) -> Tuple[float, float]:
        """Returns GARCH_7D and EGARCH_1D"""
        try:
            cache_key = "garch_multi"
            if cache_key in self.vol_cache:
                val, ts = self.vol_cache[cache_key]
                if (datetime.now(IST) - ts).total_seconds() < 300: return val

            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna() * 100
            
            # Need decent amount of data for GARCH convergence
            if len(returns) < 60: 
                # Fallback to simple std dev if not enough history for GARCH
                fallback = returns.std() * np.sqrt(252)
                return fallback, fallback

            # 1. GARCH(1,1) - 7 Day Forecast
            gm = arch_model(returns, vol='Garch', p=1, q=1, dist='t')
            res_g = gm.fit(disp='off')
            garch_7d = np.sqrt(res_g.forecast(horizon=7).variance.iloc[-1].mean()) * np.sqrt(252)

            # 2. EGARCH(1,1) - 1 Day Forecast
            em = arch_model(returns, vol='EGARCH', p=1, q=1, dist='t')
            res_e = em.fit(disp='off')
            egarch_1d = np.sqrt(res_e.forecast(horizon=1).variance.iloc[-1].iloc[0]) * np.sqrt(252)

            self.vol_cache[cache_key] = ((garch_7d, egarch_1d), datetime.now(IST))
            return garch_7d, egarch_1d
        except Exception as e: 
            # Silent fallback
            return 15.0, 15.0
