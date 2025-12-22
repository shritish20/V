# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, ANY
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SmartCapitalAllocator(100000.0, config, mock_db)
    
    # Mock the Limit Check to pass
    mock_db.get_session.return_value.__aenter__.return_value.execute.return_value.scalars.return_value.first.return_value.used_amount = 0.0
    
    # Call Allocate
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    assert result is True
    
    # Verify the SQL query contained "ON CONFLICT"
    # We check the arguments passed to session.execute
    args = mock_db.get_session.return_value.__aenter__.return_value.execute.call_args[0]
    query_text = str(args[0])
    
    assert "INSERT INTO capital_ledger" in query_text
    assert "ON CONFLICT" in query_text
    assert "DO NOTHING" in query_text
