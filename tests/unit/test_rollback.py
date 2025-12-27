import pytest
from core.engine import VolGuard20Engine, EngineCircuitBreaker
from trading.live_order_executor import RollbackFailure

@pytest.mark.asyncio
async def test_rollback_failure_halts_engine(mock_db, mock_upstox):
    engine = VolGuard20Engine()
    engine.db = mock_db
    engine.api = mock_upstox
    # force a RollbackFailure inside _trading_logic
    with pytest.raises(EngineCircuitBreaker):
        await engine._trading_logic(spot=22000)  # will trigger rollback
    assert engine.running is False
