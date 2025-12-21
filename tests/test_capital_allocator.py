import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from capital.allocator import SmartCapitalAllocator
from database.models import DbCapitalLedger
from sqlalchemy import select

@pytest.fixture
def mock_db():
    db = MagicMock()
    session = AsyncMock()
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
    session.scalar.side_effect = [None, None] 
    
    with patch.object(allocator, "_get_real_margin", return_value=100000.0):
        await allocator.allocate_capital("intraday", 5000, "T-123")
        assert session.add.called 

@pytest.mark.asyncio
async def test_draw_down_brake(allocator, mock_db):
    session = mock_db.get_session.return_value.__aenter__.return_value
    # SOD entry showing 10L
    session.scalar.return_value = DbCapitalLedger(amount=1000000.0, trade_id="SOD")
    
    # Current margin 9.5L (5% drop)
    with patch.object(allocator, "_get_real_margin", return_value=950000.0):
        result = await allocator.allocate_capital("intraday", 5000, "T-NEW")
        assert result is False 
