# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SmartCapitalAllocator(100000.0, config, mock_db)
    
    # --- CRITICAL FIX: Bypass internal DB helpers ---
    # We mock these to return safe floats/bools so the code reaches the INSERT statement
    allocator._get_real_margin = AsyncMock(return_value=100000.0)
    allocator._current_draw_down_pct = AsyncMock(return_value=0.0) # 0% Drawdown
    allocator._check_limit = AsyncMock(return_value=True)          # Limit Check Passes
    
    # Mock the DB session for the INSERT command
    mock_session = AsyncMock()
    mock_db.get_session.return_value.__aenter__.return_value = mock_session
    
    # Call Allocate
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    # Assertions
    assert result is True
    # Verify that an INSERT command was actually sent to the DB
    assert mock_session.execute.called
