from logic_core.analytics import AnalyticsEngine
from database.manager import HybridDatabaseManager

class ExecutionOrchestrator:
    def __init__(self, rest_client, sheriff, algo_tag):
        self.sheriff = sheriff
        self.client = rest_client
        self.algo_tag = algo_tag
        # lazy load executor to avoid circular imports if any
        from execution.executor import OrderExecutor
        self.executor = OrderExecutor(rest_client)

    def try_execute(self, market_state, regime, trade_state, system_health, capital_ok, strategy_orders):
        # 1. Final Sheriff Check
        allowed, reason = self.sheriff.check_system_health(0, 0), "HEALTH_CHECK"
        if not allowed:
             return {"status": "BLOCKED", "reason": "System Health Fail"}

        # 2. Execute
        if not strategy_orders:
            return {"status": "NO_ORDERS"}

        result = self.executor.execute_batch(strategy_orders, self.algo_tag)
        if result["success"]:
             return {"status": "EXECUTED", "orders": result["orders"]}
        
        return {"status": "FAILED", "reason": result.get("error")}
