# tests/unit/test_allocator.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SmartCapitalAllocator(100000.0, config, mock_db)
    
    # We use patch.object to FORCE the methods to return what we want.
    # This bypasses the 'MagicMock' pollution completely.
    
    with patch.object(allocator, '_get_real_margin', new_callable=AsyncMock) as mock_margin, \
         patch.object(allocator, '_current_draw_down_pct', new_callable=AsyncMock) as mock_dd, \
         patch.object(allocator, '_check_limit', new_callable=AsyncMock) as mock_limit:
        
        # Configure the forced mocks
        mock_margin.return_value = 100000.0
        mock_dd.return_value = 0.0      # Explicit Float 0.0
        mock_limit.return_value = True  # Explicit True
        
        # Mock the DB Session for the final INSERT check
        mock_session = AsyncMock()
        mock_db.get_session.return_value.__aenter__.return_value = mock_session
        
        # --- ACTION ---
        result = await allocator.allocate_capital("WEEKLY", 10000.0, "TRADE-123")
        
        # --- ASSERTIONS ---
        assert result is True
        assert mock_session.execute.called
