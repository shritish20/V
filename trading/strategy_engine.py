import logging
from typing import List, Tuple, Dict, Any
from datetime import datetime
from core.config import settings
from core.models import AdvancedMetrics
from core.enums import StrategyType, CapitalBucket, MarketRegime

logger = logging.getLogger("VolGuard18")

class IntelligentStrategyEngine:
    def __init__(self, vol_analytics, event_intel, sabr_model):
        self.vol_analytics = vol_analytics
        self.event_intel = event_intel
        self.sabr = sabr_model

    def select_strategy(self, metrics: AdvancedMetrics, spot: float,
                        capital_status: Dict[str, Dict[str, float]]) -> Tuple[str, List[Dict], str, CapitalBucket]:
        event_score = metrics.event_risk_score
        regime = metrics.regime
        ivp = metrics.ivp
        vix = metrics.vix
        straddle_price = self._estimate_straddle_price(spot, metrics.atm_iv)

        if event_score > 3.0:
            logger.info("High event risk - waiting")
            return StrategyType.WAIT.value, [], "WAIT", CapitalBucket.WEEKLY

        if regime == MarketRegime.PANIC:
            return self._panic_strategy(spot, straddle_price, capital_status)
        elif regime == MarketRegime.LOW_VOL_COMPRESSION:
            return self._low_vol_strategy(spot, straddle_price, capital_status)
        elif regime == MarketRegime.FEAR_BACKWARDATION:
            return self._fear_strategy(spot, straddle_price, capital_status)
        else:
            return self._neutral_strategy(spot, straddle_price, capital_status)

    def _estimate_straddle_price(self, spot: float, atm_iv: float) -> float:
        return spot * atm_iv * 0.4

    def _panic_strategy(self, spot: float, straddle_price: float, capital_status: Dict) -> Tuple[str, List[Dict], str, CapitalBucket]:
        if not self._can_allocate(CapitalBucket.WEEKLY, straddle_price * settings.LOT_SIZE, capital_status):
            return StrategyType.WAIT.value, [], "WAIT", CapitalBucket.WEEKLY

        atm_strike = round(spot / 50) * 50
        legs = [
            {"symbol": f"NIFTY{atm_strike}CE", "quantity": -1, "strike": atm_strike, "option_type": "CE"},
            {"symbol": f"NIFTY{atm_strike}PE", "quantity": -1, "strike": atm_strike, "option_type": "PE"}
        ]
        logger.info("Panic regime - selling ATM straddle")
        return StrategyType.ATM_STRADDLE.value, legs, "WEEKLY", CapitalBucket.WEEKLY

    def _low_vol_strategy(self, spot: float, straddle_price: float, capital_status: Dict) -> Tuple[str, List[Dict], str, CapitalBucket]:
        if not self._can_allocate(CapitalBucket.MONTHLY, straddle_price * settings.LOT_SIZE, capital_status):
            return StrategyType.WAIT.value, [], "WAIT", CapitalBucket.MONTHLY

        atm_strike = round(spot / 50) * 50
        legs = [
            {"symbol": f"NIFTY{atm_strike}CE", "quantity": 1, "strike": atm_strike, "option_type": "CE"},
            {"symbol": f"NIFTY{atm_strike}PE", "quantity": 1, "strike": atm_strike, "option_type": "PE"}
        ]
        logger.info("Low vol regime - buying ATM straddle")
        return StrategyType.ATM_STRADDLE.value, legs, "MONTHLY", CapitalBucket.MONTHLY

    def _fear_strategy(self, spot: float, straddle_price: float, capital_status: Dict) -> Tuple[str, List[Dict], str, CapitalBucket]:
        if not self._can_allocate(CapitalBucket.WEEKLY, straddle_price * settings.LOT_SIZE, capital_status):
            return StrategyType.WAIT.value, [], "WAIT", CapitalBucket.WEEKLY

        atm_strike = round(spot / 50) * 50
        otm_call_strike = atm_strike + 200
        otm_put_strike = atm_strike - 200
        legs = [
            {"symbol": f"NIFTY{atm_strike}CE", "quantity": -1, "strike": atm_strike, "option_type": "CE"},
            {"symbol": f"NIFTY{atm_strike}PE", "quantity": -1, "strike": atm_strike, "option_type": "PE"},
            {"symbol": f"NIFTY{otm_call_strike}CE", "quantity": 1, "strike": otm_call_strike, "option_type": "CE"},
            {"symbol": f"NIFTY{otm_put_strike}PE", "quantity": 1, "strike": otm_put_strike, "option_type": "PE"}
        ]
        logger.info("Fear regime - iron condor")
        return StrategyType.IRON_CONDOR.value, legs, "WEEKLY", CapitalBucket.WEEKLY

    def _neutral_strategy(self, spot: float, straddle_price: float, capital_status: Dict) -> Tuple[str, List[Dict], str, CapitalBucket]:
        bucket = CapitalBucket.WEEKLY if straddle_price < spot * 0.02 else CapitalBucket.MONTHLY
        if not self._can_allocate(bucket, straddle_price * settings.LOT_SIZE, capital_status):
            return StrategyType.WAIT.value, [], "WAIT", bucket

        atm_strike = round(spot / 50) * 50
        legs = [
            {"symbol": f"NIFTY{atm_strike}CE", "quantity": -1, "strike": atm_strike, "option_type": "CE"},
            {"symbol": f"NIFTY{atm_strike}PE", "quantity": -1, "strike": atm_strike, "option_type": "PE"}
        ]
        logger.info("Neutral regime - selling ATM straddle")
        return StrategyType.ATM_STRADDLE.value, legs, bucket.value.upper(), bucket

    def _can_allocate(self, bucket: CapitalBucket, required: float, capital_status: Dict) -> bool:
        available = capital_status.get("available", {}).get(bucket.value, 0)
        return available >= required
