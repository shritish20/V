#!/usr/bin/env python3
"""
SmartCapitalAllocator 20.0 – Production Test Suite
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from capital.allocator import SmartCapitalAllocator
from database.models import DbCapitalLedger

# --- FIXTURES ---
@pytest.fixture
def mock_db():
    db = MagicMock()
    # Mock the session context manager
    session = AsyncMock()
    db.get_session.return_value.__aenter__.return_value = session
    return db

@pytest.fixture
def allocator(mock_db):
    return SmartCapitalAllocator(
        fallback_account_size=2000000.0,
        allocation_config={"intraday": 0.1},
        db=mock_db
    )

# --- TESTS ---

@pytest.mark.asyncio
async def test_idempotent_allocate(allocator, mock_db):
    """
    If we call allocate twice for 'T-123', the second call should return True
    but NOT add a new row to the DB.
    """
    session = mock_db.get_session.return_value.__aenter__.return_value
    
    # Mock: First call finds nothing (New Trade)
    session.scalar.side_effect = [None, None] 
    
    with patch.object(allocator, "_get_real_margin", return_value=100000.0):
        await allocator.allocate_capital("intraday", 5000, "T-123")
        assert session.add.called # Should add to ledger
    
    # Mock: Second call finds existing row (Duplicate)
    session.reset_mock()
    session.scalar.side_effect = [DbCapitalLedger(trade_id="T-123"), None]
    
    with patch.object(allocator, "_get_real_margin", return_value=100000.0):
        await allocator.allocate_capital("intraday", 5000, "T-123")
        assert not session.add.called # Should NOT add again

@pytest.mark.asyncio
async def test_draw_down_brake(allocator, mock_db):
    """
    If current margin is 4% below Start-Of-Day margin, refuse allocation.
    """
    session = mock_db.get_session.return_value.__aenter__.return_value
    
    # Mock: SOD Margin was 10 Lakhs
    session.scalar.return_value = DbCapitalLedger(amount=1000000.0)
    
    # Mock: Current Margin is 9.5 Lakhs (5% Loss)
    # Limit is 3%
    with patch.object(allocator, "_get_real_margin", return_value=950000.0):
        result = await allocator.allocate_capital("intraday", 5000, "T-NEW")
        assert result is False # Should be blocked
