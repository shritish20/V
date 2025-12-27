import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from core.engine import VolGuard20Engine, EngineCircuitBreaker
from trading.live_order_executor import RollbackFailure

@pytest.mark.asyncio
async def test_rollback_failure_halts_engine(mock_db, mock_upstox):
    engine = VolGuard20Engine()
    engine.db = mock_db
    engine.api = mock_upstox

    # Force RollbackFailure inside _trading_logic by mocking the executor
    async def mock_execute_with_hedge_priority(trade):
        raise RollbackFailure("Simulated rollback failure")
    engine.executor.execute_with_hedge_priority = mock_execute_with_hedge_priority

    # Ensure engine is running
    engine.running = True

    # Trigger trading logic that will raise RollbackFailure
    try:
        await engine._trading_logic(spot=22000)
    except EngineCircuitBreaker:
        pass  # expected

    # Engine must have stopped itself
    assert engine.running is False
