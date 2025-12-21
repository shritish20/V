import numpy as np
import pandas as pd
from typing import Tuple
from core.config import settings
import logging

logger = logging.getLogger("VRP_ZScore")

class VRPZScoreAnalyzer:
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        
    def calculate_vrp_zscore(self, current_iv: float, current_vix: float) -> Tuple[float, str, dict]:
        try:
            nifty_df = self.data_fetcher.nifty_data
            vix_df = self.data_fetcher.vix_data
            
            # FIX: Robust check for empty or missing columns
            if nifty_df.empty or vix_df.empty: return 0.0, "NO_DATA", {}
            if 'close' not in nifty_df.columns or 'close' not in vix_df.columns: return 0.0, "BAD_SCHEMA", {}

            # Calculate returns if needed
            if 'Log_Returns' not in nifty_df.columns:
                nifty_df['Log_Returns'] = np.log(nifty_df['close'] / nifty_df['close'].shift(1))
            
            if 'RV_7D' not in nifty_df.columns:
                nifty_df['RV_7D'] = nifty_df['Log_Returns'].ewm(span=7).std() * np.sqrt(252) * 100
            
            common_idx = nifty_df.index.intersection(vix_df.index)
            if len(common_idx) < 30: return 0.0, "SHORT_HIST", {}
            
            aligned_rv = nifty_df.loc[common_idx, 'RV_7D'].dropna()
            aligned_vix = vix_df.loc[common_idx, 'close']
            
            spread_hist = aligned_vix - aligned_rv
            window = min(252, len(spread_hist))
            
            roll_mean = spread_hist.rolling(window=window).mean()
            roll_std = spread_hist.rolling(window=window).std()
            
            latest_rv = aligned_rv.iloc[-1]
            current_spread = current_vix - latest_rv 
            
            z_mean = roll_mean.iloc[-1]
            z_std = roll_std.iloc[-1]
            
            if z_std == 0 or np.isnan(z_std):
                return 0.0, "FLAT_STD", {}
            
            z_score = (current_spread - z_mean) / z_std
            if np.isnan(z_score): return 0.0, "MATH_ERR", {}
            
            signal = "SELL" if z_score > 1.0 else "BUY" if z_score < -1.0 else "NEUTRAL"
            
            return z_score, signal, {"z_score": round(z_score, 2)}
            
        except Exception as e:
            logger.error(f"Z-Score calculation failed: {e}")
            return 0.0, "ERROR", {}
