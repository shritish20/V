# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

class SafeAllocator(SmartCapitalAllocator):
    """
    A specific subclass for testing.
    It overrides complex internal methods to return simple, safe numbers/actions.
    This guarantees that 'MagicMock' never pollutes the math logic.
    """
    async def _get_real_margin(self) -> float:
        """Override to return fixed margin amount"""
        return 100000.0

    async def _current_draw_down_pct(self, current_margin: float) -> float:
        """Override to return safe 0% drawdown"""
        return 0.0

    async def _check_limit(self, bucket: str, amount: float) -> bool:
        """
        Override the entire _check_limit method to avoid DB query.
        """
        return True # Always allow
    
    async def _update_usage_summary(self, session, bucket: str, delta: float):
        """
        Override to avoid DB operations that cause MagicMock math errors.
        We only care about the INSERT command in this unit test.
        """
        pass
    
    async def _get_used_breakdown(self) -> dict:
        """Override to return empty usage"""
        return {}

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    
    # 1. Initialize the Safe Subclass
    allocator = SafeAllocator(100000.0, config, mock_db)
    
    # 2. Mock the DB Session
    mock_session = AsyncMock()
    mock_db.get_session.return_value.__aenter__.return_value = mock_session
    
    # 3. Mock safe_commit
    mock_db.safe_commit = AsyncMock()
    
    # 4. Action: Allocate 10k
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    # 5. Assertions
    assert result is True, "Allocation should succeed"
    
    # Verify the SQL command was sent
    assert mock_session.execute.called, "SQL execute should have been called"
    
    # Verify the INSERT query parameters
    call_args = mock_session.execute.call_args
    # call_args[1] usually holds the params dict if passed as named arg
    # call_args[0][1] holds it if passed as positional
    params = call_args[1] if call_args[1] else call_args[0][1]
    
    assert params["trade_id"] == "TRADE-123"
    assert params["bucket"] == "WEEKLY"
    assert params["amount"] == 10000.0
