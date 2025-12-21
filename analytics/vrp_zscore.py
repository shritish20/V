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
        self.lookback_window = 252  # 1 year for Z-score calc
        
    def calculate_vrp_zscore(self, current_iv: float, current_vix: float) -> Tuple[float, str, dict]:
        """
        Returns: (z_score, signal, details)
        """
        try:
            # 1. Get aligned data (Nifty RV + VIX history)
            nifty_df = self.data_fetcher.nifty_data
            vix_df = self.data_fetcher.vix_data
            
            if nifty_df.empty or vix_df.empty:
                logger.warning("Insufficient data for Z-Score calculation")
                return 0.0, "UNKNOWN", {}
            
            # 2. Calculate Realized Vol (7-day EWMA)
            if 'Log_Returns' not in nifty_df.columns:
                nifty_df['Log_Returns'] = np.log(nifty_df['close'] / nifty_df['close'].shift(1))
            
            nifty_df['RV_7D'] = nifty_df['Log_Returns'].ewm(span=7).std() * np.sqrt(252) * 100
            
            # 3. Align indices (dates must match)
            common_idx = nifty_df.index.intersection(vix_df.index)
            if len(common_idx) < self.lookback_window:
                logger.warning(f"Only {len(common_idx)} days available (need {self.lookback_window})")
                return 0.0, "INSUFFICIENT_DATA", {}
            
            aligned_rv = nifty_df.loc[common_idx, 'RV_7D'].dropna()
            aligned_vix = vix_df.loc[common_idx, 'close']
            
            # 4. Calculate VRP spread history (IV - RV)
            spread_hist = aligned_vix - aligned_rv
            
            # 5. Rolling statistics (1-year window)
            roll_mean = spread_hist.rolling(window=self.lookback_window).mean()
            roll_std = spread_hist.rolling(window=self.lookback_window).std()
            
            # 6. Current Z-Score
            latest_rv = aligned_rv.iloc[-1]
            current_spread = current_vix - latest_rv  # Using VIX as proxy for IV
            
            z_mean = roll_mean.iloc[-1]
            z_std = roll_std.iloc[-1]
            
            if z_std == 0 or np.isnan(z_std):
                logger.warning("Zero standard deviation in VRP history")
                return 0.0, "FLAT_HISTORY", {}
            
            z_score = (current_spread - z_mean) / z_std
            
            # 7. Generate signal
            if z_score > 2.0:
                signal = "EXTREME_SELL"
                action = "Maximum premium selling opportunity"
            elif z_score > 1.0:
                signal = "SELL"
                action = "Favorable for premium selling"
            elif z_score < -2.0:
                signal = "EXTREME_BUY"
                action = "Options extremely cheap - consider buying"
            elif z_score < -1.0:
                signal = "BUY"
                action = "Options underpriced"
            else:
                signal = "NEUTRAL"
                action = "Fair value - no statistical edge"
            
            # 8. Build details dict
            details = {
                "z_score": round(z_score, 2),
                "current_spread": round(current_spread, 2),
                "mean_spread": round(z_mean, 2),
                "std_spread": round(z_std, 2),
                "current_rv": round(latest_rv, 2),
                "current_vix": round(current_vix, 2),
                "action": action
            }
            
            logger.info(f"📊 VRP Z-Score: {z_score:.2f}σ | Signal: {signal}")
            return z_score, signal, details
            
        except Exception as e:
            logger.error(f"Z-Score calculation failed: {e}")
            return 0.0, "ERROR", {}
