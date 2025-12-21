import numpy as np
import pandas as pd
from typing import Tuple
from datetime import datetime
from core.config import settings, IST
import logging

logger = logging.getLogger("VRP_ZScore")

class VRPZScoreAnalyzer:
    """
    Volatility Risk Premium Z-Score Calculator
    Detects when implied vol is statistically expensive/cheap vs realized
    """
    
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        
    def calculate_vrp_zscore(self, current_iv: float, current_vix: float) -> Tuple[float, str, dict]:
        """
        Returns: (z_score, signal, details)
        """
        try:
            # 1. Get aligned data (Nifty RV + VIX history)
            nifty_df = self.data_fetcher.nifty_data
            vix_df = self.data_fetcher.vix_data
            
            if nifty_df.empty or vix_df.empty:
                return 0.0, "NO_DATA", {}
            
            # 2. Calculate Realized Vol (7-day EWMA)
            if 'Log_Returns' not in nifty_df.columns:
                nifty_df['Log_Returns'] = np.log(nifty_df['close'] / nifty_df['close'].shift(1))
            
            nifty_df['RV_7D'] = nifty_df['Log_Returns'].ewm(span=7).std() * np.sqrt(252) * 100
            
            # 3. Align indices (dates must match exactly)
            common_idx = nifty_df.index.intersection(vix_df.index)
            
            # Check length - we need at least some history, but don't hardcode 252
            if len(common_idx) < 30:
                logger.warning(f"Insufficient common history: {len(common_idx)} days")
                return 0.0, "INSUFFICIENT_DATA", {}
            
            aligned_rv = nifty_df.loc[common_idx, 'RV_7D'].dropna()
            aligned_vix = vix_df.loc[common_idx, 'close']
            
            # 4. Calculate VRP spread history (IV - RV)
            spread_hist = aligned_vix - aligned_rv
            
            # 5. Rolling statistics (Use whatever window we have, up to 252)
            window = min(252, len(spread_hist))
            if window < 20: return 0.0, "DATA_TOO_SHORT", {}
            
            roll_mean = spread_hist.rolling(window=window).mean()
            roll_std = spread_hist.rolling(window=window).std()
            
            # 6. Current Z-Score
            latest_rv = aligned_rv.iloc[-1]
            current_spread = current_vix - latest_rv 
            
            z_mean = roll_mean.iloc[-1]
            z_std = roll_std.iloc[-1]
            
            # Avoid division by zero
            if z_std == 0 or np.isnan(z_std) or np.isnan(z_mean):
                return 0.0, "MATH_ERROR", {}
            
            z_score = (current_spread - z_mean) / z_std
            
            # 7. Generate signal
            signal = "NEUTRAL"
            action = "Fair value"
            if z_score > 2.0:
                signal = "EXTREME_SELL"
                action = "Options Expensive"
            elif z_score > 1.0:
                signal = "SELL"
            elif z_score < -2.0:
                signal = "EXTREME_BUY"
                action = "Options Cheap"
            elif z_score < -1.0:
                signal = "BUY"
            
            # 8. Build details
            details = {
                "z_score": round(z_score, 2),
                "current_spread": round(current_spread, 2),
                "mean_spread": round(z_mean, 2),
                "std_spread": round(z_std, 2),
                "current_rv": round(latest_rv, 2),
                "action": action
            }
            
            return z_score, signal, details
            
        except Exception as e:
            logger.error(f"Z-Score calculation failed: {e}")
            return 0.0, "ERROR", {}
