import pytest
from unittest.mock import AsyncMock
from database.manager import HybridDatabaseManager
from capital.allocator import SmartCapitalAllocator

class MockAllocator(SmartCapitalAllocator):
    async def _get_real_margin(self) -> float:
        return 1_000_000.0
    async def _current_draw_down_pct(self, current_margin: float) -> float:
        return 0.0
    async def _get_used_breakdown(self) -> dict:
        return {}

@pytest.mark.asyncio
async def test_concurrent_allocation_prevents_over_leverage():
    db = HybridDatabaseManager()
    # Use the real DB but with a mock allocator that never hits external APIs
    allocator = MockAllocator(1_000_000.0, {"WEEKLY": 0.5}, db)

    # two coroutines race for 600k each (total 1.2M > 500k limit)
    tasks = [
        allocator.allocate_capital("WEEKLY", 600_000, "T1"),
        allocator.allocate_capital("WEEKLY", 600_000, "T2")
    ]
    results = await asyncio.gather(*tasks)
    assert sum(results) == 1  # exactly one succeeds
