import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class MarketState:
    spot: float
    vix: float
    rv7: float
    rv28: float
    ivp: float
    vrp_score: float
    pcr: float
    max_pain: float
    trend: str
    term_structure_slope: float

class AnalyticsEngine:
    """PURE LOGIC: Converts raw data into a MarketState object."""
    
    @staticmethod
    def calculate_rv(returns: pd.Series, window: int = 7) -> float:
        if len(returns) < window: return 0.0
        return returns.ewm(span=window).std().iloc[-1] * np.sqrt(252) * 100

    @staticmethod
    def calculate_iv_rank(current_vix: float, history: np.array) -> float:
        if len(history) < 10: return 50.0
        low, high = np.min(history), np.max(history)
        if high == low: return 50.0
        return ((current_vix - low) / (high - low)) * 100

    @staticmethod
    def analyze_trend(spot: float, history: pd.Series, window: int = 20) -> str:
        if len(history) < window: return "NEUTRAL"
        ma = history.tail(window).mean()
        if spot > ma * 1.01: return "BULLISH"
        if spot < ma * 0.99: return "BEARISH"
        return "NEUTRAL"

    @staticmethod
    def build_market_state(spot: float, vix: float, price_history: pd.DataFrame, 
                          vix_history: pd.DataFrame, chain_metrics: Dict) -> MarketState:
        
        rv7 = AnalyticsEngine.calculate_rv(price_history['log_returns'], 7)
        rv28 = AnalyticsEngine.calculate_rv(price_history['log_returns'], 28)
        
        vix_vals = vix_history['close'].values
        iv_rank = AnalyticsEngine.calculate_iv_rank(vix, vix_vals)
        
        # VRP = Implied (VIX) - Realized (RV7)
        vrp = vix - rv7 

        return MarketState(
            spot=spot, vix=vix, rv7=round(rv7, 2), rv28=round(rv28, 2),
            ivp=round(iv_rank, 2), vrp_score=round(vrp, 2),
            pcr=chain_metrics.get('pcr', 1.0),
            max_pain=chain_metrics.get('max_pain', spot),
            trend=AnalyticsEngine.analyze_trend(spot, price_history['close']),
            term_structure_slope=chain_metrics.get('slope', 0.0)
        )
