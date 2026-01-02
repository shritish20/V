import logging
from typing import Tuple, Dict, Any
from logic_core.analytics import MarketState
from logic_core.regime import RegimeDecision, RegimeClassifier
from logic_core.risk import RiskValidator

logger = logging.getLogger("Sheriff")

class Sheriff:
    """
    THE AUTHORITY: The ONLY component allowed to say "YES" or "NO" to a trade.
    """
    def __init__(self, config: Dict):
        self.config = config
        
    def assess_trade(self, market_state: MarketState, portfolio_state: Dict, 
                    trade_proposal: Dict) -> Tuple[bool, str, RegimeDecision]:
        
        # 1. Determine Regime
        regime = RegimeClassifier.classify(market_state)
        
        if regime.name == "CASH":
            return False, f"REGIME_BLOCK: {regime.reasons}", regime
            
        # 2. Validate Risk
        risk_error = RiskValidator.check_trade_limits(
            trade_greeks=trade_proposal.get('greeks', {}),
            portfolio_greeks=portfolio_state.get('greeks', {}),
            limits=self.config.get('RISK_LIMITS', {}),
            regime_allowance=regime.allowed_exposure_pct
        )
        
        if risk_error:
            return False, risk_error, regime
            
        return True, "AUTHORIZED", regime

    def check_system_health(self, heartbeat_age: float, error_count: int) -> bool:
        if heartbeat_age > 30: 
            logger.critical("SHERIFF: System Heartbeat Lost")
            return False
        return True
