from typing import Optional, Dict

class RiskValidator:
    """PURE LOGIC: Validates a proposed trade against constraints."""
    
    @staticmethod
    def check_trade_limits(trade_greeks: Dict, portfolio_greeks: Dict, 
                          limits: Dict, regime_allowance: float) -> Optional[str]:
        
        # Simple net delta check
        net_delta = abs(portfolio_greeks.get('delta', 0) + trade_greeks.get('delta', 0))
        max_delta = limits.get('MAX_DELTA', 100) * regime_allowance
        
        if net_delta > max_delta:
            return f"DELTA_BREACH: {net_delta:.1f} > {max_delta:.1f}"
            
        return None # Safe
