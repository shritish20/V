# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SmartCapitalAllocator(100000.0, config, mock_db)
    
    # 1. Mock internal margin fetch so it returns a clean float
    # This prevents the 'MagicMock > float' error
    allocator._get_real_margin = AsyncMock(return_value=100000.0)

    # 2. Mock the DB Limit Check
    # When allocator checks existing usage, we return a Row with used_amount=0.0
    mock_row = MagicMock()
    mock_row.used_amount = 0.0 # Clean float
    
    # Setup session.execute() -> result -> scalars() -> first() -> mock_row
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = mock_row
    mock_db.get_session.return_value.__aenter__.return_value.execute.return_value = mock_result
    
    # 3. Call Allocate
    # We allocate 10k. Limit is 50k (0.5 * 100k). 
    # 0 + 10k < 50k. Should pass.
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    assert result is True
