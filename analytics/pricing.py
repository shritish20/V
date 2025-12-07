import numpy as np
from scipy.stats import norm
from typing import Dict, Optional, Tuple
from datetime import datetime
import logging

from core.config import settings, IST
from core.models import GreeksSnapshot
from .sabr_model import EnhancedSABRModel

logger = logging.getLogger("VolGuard18")

class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel):
        self.sabr = sabr_model
        self.cache: Dict[str, tuple] = {}
        self.cache_ttl = 30 # seconds

    def calculate_greeks(self, spot: float, strike: float, option_type: str, 
                         expiry: str, risk_free_rate: Optional[float] = None) -> GreeksSnapshot:
        
        cache_key = f"{spot}_{strike}_{option_type}_{expiry}"
        if cache_key in self.cache:
            greeks, timestamp = self.cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return greeks

        try:
            # CRITICAL FIX: Expiry Day Math
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            now = datetime.now(IST)
            
            if expiry_dt.date() == now.date():
                # Intra-day expiry logic: 
                # Market closes at 15:30. Calculate fraction of remaining minutes.
                market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
                minutes_remaining = max(0, (market_close - now).total_seconds() / 60)
                # Use tiny floor (1 min / total yearly minutes) to prevent overflow
                time_to_expiry = max(1e-6, minutes_remaining / (365 * 24 * 60))
            else:
                time_to_expiry = max(0.001, (expiry_dt - now).days / 365.0)

            rfr = risk_free_rate or settings.RISK_FREE_RATE
            iv = self._get_implied_volatility(spot, strike, time_to_expiry)
            
            greeks = self._calculate_black_scholes_greeks(spot, strike, time_to_expiry, iv, rfr, option_type)
            
            self.cache[cache_key] = (greeks, datetime.now(IST))
            return greeks

        except Exception as e:
            logger.error(f"Greeks calculation failed: {e}")
            return GreeksSnapshot(timestamp=datetime.now(IST))

    def _get_implied_volatility(self, spot: float, strike: float, time_to_expiry: float) -> float:
        if self.sabr.calibrated:
            return self.sabr.sabr_volatility(strike, spot, time_to_expiry)
        else:
            moneyness = abs(strike - spot) / spot
            base_iv = 0.15
            skew_adjustment = 0.02 * moneyness * 100
            return min(0.80, max(0.05, base_iv + skew_adjustment))

    def _calculate_black_scholes_greeks(self, spot: float, strike: float, time_to_expiry: float, 
                                        iv: float, risk_free_rate: float, option_type: str) -> GreeksSnapshot:
        
        # Safety clamps
        time_to_expiry = max(1e-6, time_to_expiry)
        iv = max(0.01, iv)

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
        
        # Additional Greeks
        pop = norm.cdf(d2 if option_type == 'CE' else -d2)
        
        # Vanna/Charm formulas omitted for brevity, keeping 0 default as they are nice-to-have
        
        return GreeksSnapshot(
            timestamp=datetime.now(IST),
            delta=delta,
            gamma=gamma,
            theta=theta / 365, # Theta is usually annualized, dividing for daily view
            vega=vega / 100, # Vega is usually per 1% change
            iv=iv,
            pop=pop,
        )

    def calculate_option_price(self, spot: float, strike: float, option_type: str, expiry: str,
                               risk_free_rate: Optional[float] = None) -> float:
        try:
            greeks = self.calculate_greeks(spot, strike, option_type, expiry, risk_free_rate)
            # Rough estimation or use BS Price formula if needed. 
            # For pricing engine, we usually care about Greeks.
            # Using IV from greeks calc to re-run BS price
            # ... implementation ...
            return 0.0 # Placeholder if not strictly needed by Engine
        except Exception:
            return 0.0
