import numpy as np
from scipy.stats import norm
from typing import Dict, Optional
from datetime import datetime
from core.config import settings, IST
from core.models import GreeksSnapshot
from .sabr_model import EnhancedSABRModel

logger = logging.getLogger("VolGuard18")

class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel):
        self.sabr = sabr_model
        self.cache: Dict[str, tuple] = {}
        self.cache_ttl = 30  # seconds

    def calculate_greeks(self, spot: float, strike: float, option_type: str, expiry: str,
                         risk_free_rate: Optional[float] = None) -> GreeksSnapshot:
        cache_key = f"{spot}_{strike}_{option_type}_{expiry}"
        if cache_key in self.cache:
            greeks, timestamp = self.cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return greeks

        try:
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            time_to_expiry = max(0.001, (expiry_dt - datetime.now(IST)).days / 365.0)
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
        time_to_expiry = max(0.001, time_to_expiry)
        iv = max(0.001, iv)

        d1 = (np.log(spot / strike) + (risk_free_rate + 0.5 * iv ** 2) * time_to_expiry) / (iv * np.sqrt(time_to_expiry))
        d2 = d1 - iv * np.sqrt(time_to_expiry)

        if option_type == 'CE':
            delta = norm.cdf(d1)
            gamma = norm.pdf(d1) / (spot * iv * np.sqrt(time_to_expiry))
            theta = (-spot * norm.pdf(d1) * iv / (2 * np.sqrt(time_to_expiry)) -
                     risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2))
            vega = spot * norm.pdf(d1) * np.sqrt(time_to_expiry)
        else:
            delta = norm.cdf(d1) - 1
            gamma = norm.pdf(d1) / (spot * iv * np.sqrt(time_to_expiry))
            theta = (-spot * norm.pdf(d1) * iv / (2 * np.sqrt(time_to_expiry)) +
                     risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2))
            vega = spot * norm.pdf(d1) * np.sqrt(time_to_expiry)

        pop = norm.cdf(d2 if option_type == 'CE' else -d2)
        charm = -norm.pdf(d1) * (2 * risk_free_rate * time_to_expiry - d2 * iv * np.sqrt(time_to_expiry)) / (2 * time_to_expiry * iv * np.sqrt(time_to_expiry))
        vanna = -norm.pdf(d1) * d2 / iv

        return GreeksSnapshot(
            timestamp=datetime.now(IST),
            delta=delta,
            gamma=gamma,
            theta=theta / 100,
            vega=vega / 100,
            iv=iv,
            pop=pop,
            charm=charm,
            vanna=vanna
        )

    def calculate_option_price(self, spot: float, strike: float, option_type: str, expiry: str,
                               risk_free_rate: Optional[float] = None) -> float:
        try:
            greeks = self.calculate_greeks(spot, strike, option_type, expiry, risk_free_rate)
            moneyness = abs(strike - spot) / spot
            time_to_expiry = max(0.001, (datetime.strptime(expiry, "%Y-%m-%d") - datetime.now(IST)).days / 365.0)
            intrinsic = max(0, spot - strike if option_type == 'CE' else strike - spot)
            time_value = spot * greeks.iv * np.sqrt(time_to_expiry) * 0.4
            return max(intrinsic, time_value * (1 - moneyness))
        except Exception as e:
            logger.error(f"Option price calculation failed: {e}")
            return 0.0

    def clear_cache(self):
        self.cache.clear()
        logger.debug("Pricing cache cleared")

    def get_cache_stats(self):
        return {'cache_size': len(self.cache), 'cache_ttl': self.cache_ttl}
