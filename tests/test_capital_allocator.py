import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from capital.allocator import SmartCapitalAllocator
from database.models import DbCapitalLedger

@pytest.fixture
def mock_db():
    db = MagicMock()
    session = AsyncMock()
    # Ensure session.execute returns a mock that can be awaited
    mock_result = MagicMock()
    mock_result.scalars.return_value = []
    session.execute = AsyncMock(return_value=mock_result)
    session.scalar = AsyncMock(return_value=None)
    
    db.get_session.return_value.__aenter__.return_value = session
    db.safe_commit = AsyncMock()
    return db

@pytest.fixture
def allocator(mock_db):
    return SmartCapitalAllocator(
        fallback_account_size=2000000.0,
        allocation_config={"intraday": 0.1},
        db=mock_db
    )

@pytest.mark.asyncio
async def test_idempotent_allocate(allocator, mock_db):
    session = mock_db.get_session.return_value.__aenter__.return_value
    with patch.object(allocator, "_get_real_margin", return_value=100000.0):
        # Should execute without error
        await allocator.allocate_capital("intraday", 5000, "T-123")
        assert session.add.called

@pytest.mark.asyncio
async def test_draw_down_brake(allocator, mock_db):
    session = mock_db.get_session.return_value.__aenter__.return_value
    # Mock Start of Day Balance as 1,000,000
    session.scalar.return_value = DbCapitalLedger(amount=1000000.0, trade_id="SOD")
    
    # Current margin 900,000 (10% drop - exceeds 3% limit)
    with patch.object(allocator, "_get_real_margin", return_value=900000.0):
        result = await allocator.allocate_capital("intraday", 5000, "T-NEW")
        assert result is False
