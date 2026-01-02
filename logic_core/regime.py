from dataclasses import dataclass
from typing import List
from .analytics import MarketState

@dataclass
class RegimeDecision:
    name: str  # AGGRESSIVE | MODERATE | DEFENSIVE | CASH
    score: float
    allowed_exposure_pct: float
    reasons: List[str]

class RegimeClassifier:
    """PURE LOGIC: Maps MarketState -> Permissions."""
    @staticmethod
    def classify(state: MarketState) -> RegimeDecision:
        reasons = []
        score = 50.0 # Base Neutral
        
        if state.vix > 24.0:
            reasons.append("VIX_PANIC")
            return RegimeDecision("CASH", 0.0, 0.0, reasons)
        elif state.vix < 11.0:
            reasons.append("VIX_COMPLACENT")
            score -= 20
        
        if state.vrp_score > 3.0:
            reasons.append("HIGH_PREMIUM")
            score += 20
        elif state.vrp_score < -1.0:
            reasons.append("NEGATIVE_CARRY")
            score -= 30 
            
        if state.trend == "BEARISH":
            reasons.append("BEAR_TREND")
            score -= 10
            
        if score >= 70: return RegimeDecision("AGGRESSIVE", score, 1.0, reasons)
        elif score >= 40: return RegimeDecision("MODERATE", score, 0.6, reasons)
        elif score >= 20: return RegimeDecision("DEFENSIVE", score, 0.3, reasons)
        else: return RegimeDecision("CASH", score, 0.0, reasons)
