import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

class SafeAllocator(SmartCapitalAllocator):
    """Test subclass that bypasses DB/margin calls"""
    async def _get_real_margin(self) -> float:
        return 100_000.0
    async def _current_draw_down_pct(self, current_margin: float) -> float:
        return 0.0
    async def _get_used_breakdown(self) -> dict:
        return {}

@pytest.mark.asyncio
async def test_atomic_allocation(mock_db):
    """Verify that allocation uses SQL INSERT ON CONFLICT."""
    config = {"WEEKLY": 0.5}
    allocator = SafeAllocator(100_000.0, config, mock_db)

    # Mock DB session
    mock_session = AsyncMock()
    mock_db.get_session.return_value.__aenter__.return_value = mock_session
    mock_db.safe_commit = AsyncMock()

    # Mock row returned by SELECT FOR UPDATE
    mock_row = (0.0,)   # current_used
    mock_session.execute = AsyncMock()
    mock_session.execute.return_value.fetchone = MagicMock(return_value=mock_row)

    # Action: Allocate 10k
    result = await allocator.allocate_capital("WEEKLY", 10_000.0, "TRADE-123")
    assert result is True
    assert mock_session.execute.called

@pytest.mark.asyncio
async def test_capital_allocation_no_deadlock(mock_db):
    """Verify that failed lock doesn't cause deadlock."""
    from capital.allocator import SmartCapitalAllocator
    import asyncio
    allocator = SmartCapitalAllocator(1_000_000.0, {"WEEKLY": 0.5}, mock_db)
    allocator._cached_available_margin = 1_000_000.0

    lock_acquired = asyncio.Event()
    lock_released = asyncio.Event()

    async def slow_allocation():
        async def mock_execute_slow(stmt, params=None):
            if "FOR UPDATE" in str(stmt):
                lock_acquired.set()
                await asyncio.sleep(2)
                raise Exception("Simulated failure")
            return MagicMock()
        mock_session = AsyncMock()
        mock_session.execute = mock_execute_slow
        mock_db.get_session.return_value.__aenter__.return_value = mock_session
        try:
            await allocator.allocate_capital("WEEKLY", 100_000.0, "SLOW TRADE")
        except:
            pass
        finally:
            lock_released.set()

    async def fast_allocation():
        await lock_acquired.wait()
        async def mock_execute_normal(stmt, params=None):
            if "FOR UPDATE" in str(stmt):
                await lock_released.wait()
                result = MagicMock()
                result.fetchone.return_value = (0.0,)
                return result
            return MagicMock()
        mock_session = AsyncMock()
        mock_session.execute = mock_execute_normal
        mock_db.get_session.return_value.__aenter__.return_value = mock_session
        mock_db.safe_commit = AsyncMock()
        return await allocator.allocate_capital("WEEKLY", 100_000.0, "FAST TRADE")

    # Run concurrently with timeout
    try:
        slow_task = asyncio.create_task(slow_allocation())
        fast_task = asyncio.create_task(fast_allocation())
        result = await asyncio.wait_for(fast_task, timeout=5.0)
        assert result == True
    except asyncio.TimeoutError:
        pytest.fail("DEADLOCK DETECTED")
