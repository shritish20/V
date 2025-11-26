import pandas as pd
import numpy as np
from arch import arch_model
import logging
from typing import Tuple
from datetime import datetime
from .sabr_model import EnhancedSABRModel
from core.config import VIX_HISTORY_URL, NIFTY_HISTORY_URL

logger = logging.getLogger("VolGuardHybrid")

class HybridVolatilityAnalytics:
    """Combines speed of Race Edition with depth of Luxury Edition"""
    
    def __init__(self):
        self.vix_data = pd.DataFrame()
        self.nifty_data = pd.DataFrame()
        self.sabr_model = EnhancedSABRModel()
        self._load_historical_data()
    
    def _load_historical_data(self):
        """Load or create realistic historical data"""
        try:
            # Try to load from URLs
            vix_df = pd.read_csv(VIX_HISTORY_URL)
            vix_df['Date'] = pd.to_datetime(vix_df['Date'], format='%d-%b-%Y', errors='coerce')
            vix_df = vix_df.sort_values('Date').dropna(subset=['Date'])
            vix_df['Close'] = pd.to_numeric(vix_df['Close'], errors='coerce')
            self.vix_data = vix_df.dropna(subset=['Close'])

            nifty_df = pd.read_csv(NIFTY_HISTORY_URL)
            nifty_df['Date'] = pd.to_datetime(nifty_df['Date'], format='%d-%b-%Y', errors='coerce')
            nifty_df = nifty_df.sort_values('Date').dropna(subset=['Date'])
            nifty_df['Close'] = pd.to_numeric(nifty_df['Close'], errors='coerce')
            self.nifty_data = nifty_df.dropna(subset=['Close'])
            
            logger.info(f"Loaded {len(self.vix_data)} VIX and {len(self.nifty_data)} Nifty records")
            
        except Exception as e:
            logger.warning(f"Historical data load failed, using synthetic data: {e}")
            self._create_synthetic_data()
    
    def _create_synthetic_data(self):
        """Create realistic synthetic data"""
        dates = pd.date_range(start='2020-01-01', end=datetime.now().strftime('%Y-%m-%d'), freq='D')
        
        # Realistic VIX pattern with mean reversion
        vix_values = 15 + 5 * np.sin(np.arange(len(dates)) * 2 * np.pi / 252) + np.random.normal(0, 2, len(dates))
        
        # Realistic Nifty growth with volatility clusters
        returns = np.random.normal(0.0005, 0.015, len(dates))
        # Add some volatility clustering
        for i in range(10, len(returns)):
            if abs(returns[i-1]) > 0.02:
                returns[i] *= 1.5
        nifty_values = 10000 * np.exp(np.cumsum(returns))
        
        self.vix_data = pd.DataFrame({'Date': dates, 'Close': np.clip(vix_values, 10, 40)})
        self.nifty_data = pd.DataFrame({'Date': dates, 'Close': nifty_values})
    
    def calculate_realized_volatility(self, window: int = 7) -> float:
        """Calculate realized volatility from Nifty returns"""
        try:
            if self.nifty_data.empty:
                return 15.0
                
            returns = np.log(self.nifty_data['Close'] / self.nifty_data['Close'].shift(1))
            recent_returns = returns.tail(window).dropna()
            
            if len(recent_returns) < 5:
                return 15.0
                
            realized_vol = np.std(recent_returns) * np.sqrt(252) * 100
            return float(np.clip(realized_vol, 5, 60))
            
        except Exception as e:
            logger.error(f"Realized vol calculation failed: {e}")
            return 15.0
    
    def calculate_garch_volatility(self, horizon: int = 7) -> float:
        """GARCH volatility forecasting"""
        try:
            if self.nifty_data.empty or len(self.nifty_data) < 100:
                return 15.0

            returns = np.log(self.nifty_data['Close'] / self.nifty_data['Close'].shift(1))
            returns = returns.dropna()
            
            if len(returns) < 100:
                return 15.0

            model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='normal')
            fitted_model = model.fit(disp='off', show_warning=False)
            
            forecast = fitted_model.forecast(horizon=horizon, reindex=False)
            garch_vol = np.sqrt(forecast.variance.iloc[-1].mean()) / 100
            annualized_vol = garch_vol * np.sqrt(252) * 100
            
            return float(np.clip(annualized_vol, 5, 60))
            
        except Exception as e:
            logger.error(f"GARCH calculation failed: {e}")
            return 15.0
    
    def calculate_iv_percentile(self, current_vix: float, lookback_days: int = 252) -> float:
        """Calculate IV Percentile"""
        try:
            if self.vix_data.empty:
                return 50.0

            recent_vix = self.vix_data.tail(lookback_days)["Close"]
            ivp = (recent_vix < current_vix).mean() * 100.0
            return float(max(0.0, min(100.0, ivp)))
            
        except Exception:
            return 50.0

    def get_volatility_metrics(self, current_vix: float) -> Tuple[float, float, float]:
        """Get comprehensive volatility metrics"""
        realized_vol = self.calculate_realized_volatility()
        garch_vol = self.calculate_garch_volatility()
        ivp = self.calculate_iv_percentile(current_vix)
        
        return realized_vol, garch_vol, ivp
