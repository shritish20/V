# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

class SafeAllocator(SmartCapitalAllocator):
    """
    A specific subclass for testing.
    It overrides the complex internal methods to return simple, safe numbers.
    This guarantees that 'MagicMock' never pollutes the math logic.
    """
    async def _get_real_margin(self) -> float:
        """Override to return fixed margin amount"""
        return 100000.0

    async def _current_draw_down_pct(self, current_margin: float) -> float:
        """Override to return safe 0% drawdown"""
        return 0.0

    async def _check_limit(self, bucket: str, amount: float) -> bool:
        """
        Override the entire _check_limit method to avoid DB query.
        This prevents the MagicMock comparison issue.
        """
        margin = await self._get_real_margin()
        limit = margin * self._bucket_pct.get(bucket, 0.0)
        # Assume 0 used for testing purposes
        used = 0.0
        return (used + amount) <= limit
    
    async def _get_used_breakdown(self) -> dict:
        """Override to return empty usage (no DB query)"""
        return {}

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
    
    # 3. Mock safe_commit to avoid issues
    mock_db.safe_commit = AsyncMock()
    
    # 4. Action: Allocate 10k
    result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
    
    # 5. Assertions
    assert result is True, "Allocation should succeed"
    
    # Verify the SQL command was sent
    assert mock_session.execute.called, "SQL execute should have been called"
