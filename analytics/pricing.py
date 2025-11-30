import math 
import numpy as np
from scipy.stats import norm
from datetime import datetime
from typing import Dict, Optional
from threading import Lock
from core.models import GreeksSnapshot
from core.config import IST, RISK_FREE_RATE, TRADING_DAYS
from .sabr_model import EnhancedSABRModel
import logging

logger = logging.getLogger("VolGuard14")

class HybridPricingEngine:
    """Advanced pricing with SABR and market data"""
    
    def __init__(self, sabr_model: EnhancedSABRModel):
        self.sabr = sabr_model
        self._cache: Dict[tuple, GreeksSnapshot] = {}
        self._cache_lock = Lock()
        self._cache_ttl = 300 # 5 minutes
        self._cache_hits = 0
        self._cache_misses = 0

    def calculate_greeks(self, spot: float, strike: float, opt_type: str, expiry: str, market_price: float = None) -> GreeksSnapshot:
        """Calculate Greeks using SABR volatility with bounds checking"""
        # Add bounds checking for extreme scenarios
        if spot <= 0 or strike <= 0:
            return GreeksSnapshot(timestamp=datetime.now(IST))
            
        moneyness = spot / strike
        if moneyness < 0.5 or moneyness > 2.0:  # Extreme OTM/ITM
            logger.warning(f"Extreme moneyness detected: {moneyness:.2f}")
            return self._calculate_extreme_greeks(spot, strike, opt_type, expiry)
        
        cache_key = (spot, strike, opt_type, expiry)
        with self._cache_lock:
            if cache_key in self._cache:
                cached = self._cache[cache_key]
                if not cached.is_stale(self._cache_ttl):
                    self._cache_hits += 1
                    return cached
            self._cache_misses += 1

        T = self._get_dte(expiry)
        iv = self.sabr.sabr_volatility(spot, strike, T)
        
        if T <= 0.001 or iv <= 0.001:
            return GreeksSnapshot(timestamp=datetime.now(IST)) 

        d1 = (math.log(spot/strike) + (RISK_FREE_RATE + 0.5*iv**2)*T) / (iv*math.sqrt(T))
        d2 = d1 - iv*math.sqrt(T)

        if opt_type == "CE":
            delta = norm.cdf(d1)
            theta = (-spot * norm.pdf(d1) * iv / (2*math.sqrt(T)) - RISK_FREE_RATE * strike * math.exp(-RISK_FREE_RATE*T) * norm.cdf(d2)) / TRADING_DAYS
        else: # PE
            delta = norm.cdf(d1) - 1
            theta = (-spot * norm.pdf(d1) * iv / (2*math.sqrt(T)) + RISK_FREE_RATE * strike * math.exp(-RISK_FREE_RATE*T) * norm.cdf(-d2)) / TRADING_DAYS
            
        gamma = norm.pdf(d1) / (spot * iv * math.sqrt(T))
        vega = spot * norm.pdf(d1) * math.sqrt(T) / 100 

        greeks = GreeksSnapshot(
            timestamp=datetime.now(IST),
            delta=delta,
            gamma=gamma,
            theta=theta,
            vega=vega
        )
        with self._cache_lock:
            self._cache[cache_key] = greeks
        
        self._clean_cache()
        return greeks

    def _calculate_extreme_greeks(self, spot: float, strike: float, opt_type: str, expiry: str) -> GreeksSnapshot:
        """Simplified Greeks for extreme OTM/ITM options"""
        T = self._get_dte(expiry)
        if T <= 0.001:
            return GreeksSnapshot(timestamp=datetime.now(IST))
            
        if opt_type == "CE":
            if spot > strike * 1.5:  # Deep ITM call
                delta = 0.99
                theta = -0.1
            else:  # Deep OTM call
                delta = 0.01
                theta = -0.01
        else:  # PE
            if spot < strike * 0.7:  # Deep ITM put
                delta = -0.99
                theta = -0.1
            else:  # Deep OTM put
                delta = -0.01
                theta = -0.01
                
        return GreeksSnapshot(
            timestamp=datetime.now(IST),
            delta=delta,
            gamma=0.001,
            theta=theta,
            vega=0.1
        )

    def _get_dte(self, expiry_str: str) -> float:
        """Get days to expiry as year fraction"""
        try:
            exp_dt = datetime.strptime(expiry_str, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=IST)
            now = datetime.now(IST)
            seconds = max(0, (exp_dt - now).total_seconds())
            return seconds / (365 * 24 * 3600)
        except:
            return 7/365

    def _clean_cache(self):
        """Clean stale cache entries"""
        self._cache = {
            k: v for k, v in self._cache.items() if not v.is_stale(self._cache_ttl)
        }

    def get_cache_stats(self) -> Dict[str, float]:
        """Get cache performance statistics"""
        total = self._cache_hits + self._cache_misses
        hit_ratio = self._cache_hits / total if total > 0 else 0
        return {
            "hit_ratio": hit_ratio,
            "cache_size": len(self._cache),
            "hits": self._cache_hits,
            "misses": self._cache_misses
        }
