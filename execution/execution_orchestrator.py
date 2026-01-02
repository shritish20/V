from datetime import datetime
from logic_core.analytics import AnalyticsEngine
from database.manager import HybridDatabaseManager
# Lazy import to avoid circular dependency if strictly typed, 
# but here we assume classes are loaded or interfaces used.

class ExecutionOrchestrator:
    def __init__(self, rest_client, sheriff, algo_tag):
        self.sheriff = sheriff
        self.client = rest_client
        self.algo_tag = algo_tag
        
        # Lazy load executor
        from execution.executor import OrderExecutor
        from execution.exit_engine import ExitEngine
        self.executor = OrderExecutor(rest_client)
        self.exit_engine = ExitEngine(rest_client)

    def try_execute(self, market_state, regime, trade_state, system_health, capital_ok, strategy_orders):
        """
        The CRITICAL choke point. 
        Sheriff must explicitly APPROVE before any order is sent.
        """

        # -----------------------------------------
        # 1. THE INTERROGATION (Fix 1: Sheriff Gate)
        # -----------------------------------------
        # We construct a proposal. If orders are empty, we still ask Sheriff 
        # to validate the 'Do Nothing' state if needed, or we handle it below.
        
        trade_proposal = {
            "order_count": len(strategy_orders),
            "orders": strategy_orders,
            # In a real scenario, calculate proposal greeks here
            "greeks": {"delta": 0, "gamma": 0, "vega": 0, "theta": 0} 
        }

        allowed, reason, approved_regime = self.sheriff.assess_trade(
            market_state=market_state,
            portfolio_state=trade_state.__dict__ if trade_state else {},
            trade_proposal=trade_proposal
        )

        # -----------------------------------------
        # 2. THE VERDICT
        # -----------------------------------------
        if not allowed:
            return {
                "status": "BLOCKED", 
                "reason": reason, 
                "regime": approved_regime.name,
                "timestamp": datetime.utcnow().isoformat()
            }

        # -----------------------------------------
        # 3. EMPTY ORDER HANDLING (Fix 2: Explicit State)
        # -----------------------------------------
        if not strategy_orders:
            return {
                "status": "NO_TRADE",
                "reason": "Strategy generated 0 orders",
                "regime": approved_regime.name,
                "timestamp": datetime.utcnow().isoformat()
            }

        # -----------------------------------------
        # 4. EXECUTION (Only if Sheriff said YES)
        # -----------------------------------------
        result = self.executor.execute_batch(strategy_orders, self.algo_tag)
        
        if result["success"]:
             return {
                 "status": "EXECUTED", 
                 "orders": result["orders"],
                 "regime": approved_regime.name
             }
        
        return {
            "status": "FAILED", 
            "reason": result.get("error")
        }
