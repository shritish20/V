import numpy as np
import logging
from datetime import datetime
from typing import Tuple, Dict
from arch import arch_model
from scipy.stats import percentileofscore
from core.config import settings, IST
from utils.data_fetcher import DashboardDataFetcher # Existing dependency

logger = logging.getLogger("VolAnalytics")

class HybridVolatilityAnalytics:
    def __init__(self):
        self.data_fetcher = DashboardDataFetcher()
        self.vol_cache: Dict[str, Tuple[float, datetime]] = {}

    def get_volatility_metrics(self, current_vix: float) -> Tuple[float, float, float]:
        """Returns: RealizedVol, GarchForecast, IVRank"""
        try:
            realized_vol = self._calculate_realized_volatility()
            garch_vol = self._calculate_garch_forecast()
            iv_rank = self._calculate_iv_rank(current_vix)
            return realized_vol, garch_vol, iv_rank
        except Exception as e:
            logger.error(f"Vol Metrics Failed: {e}")
            return 15.0, 15.0, 50.0

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
            if df.empty or 'Close' not in df.columns: return "NEUTRAL"
            
            ma20 = df['Close'].tail(20).mean()
            if spot > ma20 * 1.01: return "BULL_TREND"
            if spot < ma20 * 0.99: return "BEAR_TREND"
            return "NEUTRAL"
        except: return "NEUTRAL"

    def _calculate_iv_rank(self, current_vix: float) -> float:
        try:
            if self.data_fetcher.vix_data.empty: return 50.0
            history = self.data_fetcher.vix_data['Close'].tail(252).values
            if len(history) < 10: return 50.0
            
            return float(percentileofscore(history, current_vix, kind='weak'))
        except: return 50.0

    def _calculate_realized_volatility(self) -> float:
        try:
            if 'Log_Returns' not in self.data_fetcher.nifty_data.columns: return 15.0
            returns = self.data_fetcher.nifty_data['Log_Returns'].tail(20)
            return returns.std() * np.sqrt(252) * 100
        except: return 15.0

    def _calculate_garch_forecast(self) -> float:
        try:
            cache_key = "garch_1"
            if cache_key in self.vol_cache:
                val, ts = self.vol_cache[cache_key]
                if (datetime.now(IST) - ts).total_seconds() < 300: return val

            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna() * 100
            if len(returns) < 126: return 15.0
            
            model = arch_model(returns, vol='Garch', p=1, q=1)
            res = model.fit(disp='off')
            fc = np.sqrt(res.forecast(horizon=1).variance.values[-1, -1]) * np.sqrt(252)
            
            self.vol_cache[cache_key] = (fc, datetime.now(IST))
            return fc
        except: return 15.0
