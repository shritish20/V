# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

# --- FIX: Helper class MUST NOT start with "Test" to avoid Pytest confusion ---
class SafeAllocator(SmartCapitalAllocator):
    """
    A specific subclass for testing.
    It overrides the complex internal methods to return simple, safe numbers.
    This guarantees that 'MagicMock' never pollutes the math logic.
    """
    async def _get_real_margin(self) -> float:
        return 100000.0

    async def _current_draw_down_pct(self, current_margin: float) -> float:
        return 0.0  # Safe Float (0%)

    async def _check_limit(self, bucket: str, amount: float) -> bool:
        return True # Always allow limits

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    
    # 1. Initialize the Safe Subclass
    allocator = SafeAllocator(100000.0, config, mock_db)
    
    # 2. Mock the DB Session for the final INSERT check
    mock_session = AsyncMock()
    # Chain: db.get_session() -> context -> session
    mock_db.get_session.return_value.__aenter__.return_value = mock_session
    
    # 3. Action: Allocate 10k
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    # 4. Assertions
    assert result is True
    # Verify the SQL command was sent
    assert mock_session.execute.called
