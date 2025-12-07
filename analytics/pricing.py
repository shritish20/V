import numpy as np
from scipy.stats import norm
from typing import Dict, Optional
from datetime import datetime, time as dtime
import logging

from core.config import settings, IST
from core.models import GreeksSnapshot
from .sabr_model import EnhancedSABRModel

logger = logging.getLogger("VolGuard18")

class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel):
        self.sabr = sabr_model
        self.cache: Dict[str, tuple] = {}
        self.cache_ttl = 30

    def calculate_greeks(self, spot: float, strike: float, option_type: str, 
                         expiry: str, risk_free_rate: Optional[float] = None) -> GreeksSnapshot:
        
        cache_key = f"{spot}_{strike}_{option_type}_{expiry}"
        if cache_key in self.cache:
            greeks, timestamp = self.cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return greeks

        try:
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            now = datetime.now(IST)
            
            # FIX: High Precision Time-To-Expiry
            if expiry_dt.date() == now.date():
                # Market closes at 15:30. 
                market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
                seconds_remaining = max(0, (market_close - now).total_seconds())
                # Normalize by seconds in a year (365 * 24 * 3600)
                # Ensure minimum 1e-6 to avoid div/0
                time_to_expiry = max(1e-6, seconds_remaining / 31536000.0)
            else:
                # Include time component for greater accuracy near expiry
                expiry_target = datetime.combine(expiry_dt.date(), dtime(15, 30))
                # Localize if using offset-aware datetimes, but usually naive math works if both are same
                delta = expiry_target - now.replace(tzinfo=None) # simplistic
                time_to_expiry = max(0.001, delta.total_seconds() / 31536000.0)

            rfr = risk_free_rate or settings.RISK_FREE_RATE
            iv = self._get_implied_volatility(spot, strike, time_to_expiry)
            
            greeks = self._calculate_black_scholes_greeks(spot, strike, time_to_expiry, iv, rfr, option_type)
            self.cache[cache_key] = (greeks, datetime.now(IST))
            return greeks

        except Exception as e:
            logger.error(f"Greeks Calc Error: {e}")
            return GreeksSnapshot(timestamp=datetime.now(IST))

    def _get_implied_volatility(self, spot: float, strike: float, time_to_expiry: float) -> float:
        # Fallback Logic included
        if self.sabr.calibrated:
            return self.sabr.sabr_volatility(strike, spot, time_to_expiry)
        else:
            moneyness = abs(strike - spot) / spot
            base_iv = 0.15
            skew_adjustment = 0.02 * moneyness * 100
            return min(0.80, max(0.05, base_iv + skew_adjustment))

    def _calculate_black_scholes_greeks(self, spot: float, strike: float, time_to_expiry: float, 
                                        iv: float, risk_free_rate: float, option_type: str) -> GreeksSnapshot:
        
        iv = max(0.01, iv) # Safety floor
        
        d1 = (np.log(spot / strike) + (risk_free_rate + 0.5 * iv ** 2) * time_to_expiry) / (iv * np.sqrt(time_to_expiry))
        d2 = d1 - iv * np.sqrt(time_to_expiry)

        if option_type == 'CE':
            delta = norm.cdf(d1)
            theta = (-spot * norm.pdf(d1) * iv / (2 * np.sqrt(time_to_expiry)) - 
                     risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2))
        else:
            delta = norm.cdf(d1) - 1
            theta = (-spot * norm.pdf(d1) * iv / (2 * np.sqrt(time_to_expiry)) + 
                     risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2))

        gamma = norm.pdf(d1) / (spot * iv * np.sqrt(time_to_expiry))
        vega = spot * norm.pdf(d1) * np.sqrt(time_to_expiry)
        pop = norm.cdf(d2 if option_type == 'CE' else -d2)
        
        return GreeksSnapshot(
            timestamp=datetime.now(IST),
            delta=delta,
            gamma=gamma,
            theta=theta / 365,
            vega=vega / 100,
            iv=iv,
            pop=pop
        )
