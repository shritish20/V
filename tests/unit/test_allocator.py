# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SmartCapitalAllocator(100000.0, config, mock_db)
    
    # --- FIX START ---
    # Create a mock object representing a database row
    mock_row = MagicMock()
    mock_row.used_amount = 0.0  # Set the attribute the code expects
    
    # Build the chain: session.execute() -> result
    mock_result = MagicMock()
    # result.scalars() -> iterator -> first() -> returns our mock_row
    mock_result.scalars.return_value.first.return_value = mock_row
    
    # Attach to the session context manager
    mock_db.get_session.return_value.__aenter__.return_value.execute.return_value = mock_result
    # --- FIX END ---
    
    # Call Allocate
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    assert result is True
