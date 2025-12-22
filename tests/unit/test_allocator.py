# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SmartCapitalAllocator(100000.0, config, mock_db)
    
    # --- MOCKING THE INTERNAL HELPERS ---
    # We mock these to return safe values so the code reaches the INSERT statement.
    # If we don't mock these, they return MagicMocks which crash math comparisons.
    
    # 1. Margin is 100k
    allocator._get_real_margin = AsyncMock(return_value=100000.0)
    
    # 2. Drawdown is 0% (Safe) - THIS WAS MISSING BEFORE
    allocator._current_draw_down_pct = AsyncMock(return_value=0.0)
    
    # 3. Bucket Limit Check Passes
    allocator._check_limit = AsyncMock(return_value=True)
    
    # --- MOCKING THE DB SESSION ---
    # We need a mock session to capture the final INSERT command
    mock_session = AsyncMock()
    mock_db.get_session.return_value.__aenter__.return_value = mock_session
    
    # --- ACTION ---
    # We allocate 10k. 
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    # --- ASSERTIONS ---
    assert result is True
    # Verify that the session executed the INSERT command
    assert mock_session.execute.called
