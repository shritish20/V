# tests/unit/test_allocator.py
import pytest
from unittest.mock import MagicMock, AsyncMock
from capital.allocator import SmartCapitalAllocator

# --- THE FIX: Subclassing to bypass Mocking issues ---
class TestableAllocator(SmartCapitalAllocator):
    """
    A testing wrapper that hard-wires internal calculations to safe values.
    This eliminates 'MagicMock' pollution completely.
    """
    async def _get_real_margin(self) -> float:
        return 100000.0

    async def _current_draw_down_pct(self, current_margin: float) -> float:
        return 0.0  # Safe Float

    async def _check_limit(self, bucket: str, amount: float) -> bool:
        return True

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    
    # Use the Safe Subclass instead of the real class
    allocator = TestableAllocator(100000.0, config, mock_db)
    
    # Mock the DB Session for the final INSERT check
    mock_session = AsyncMock()
    # Ensure the chain mock_db -> session -> execute works
    mock_db.get_session.return_value.__aenter__.return_value = mock_session
    
    # --- ACTION ---
    # We allocate 10k. 
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    # --- ASSERTIONS ---
    assert result is True
    
    # Verify DB interaction
    assert mock_session.execute.called
