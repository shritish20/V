import asyncio, pytest
from capital.allocator import SmartCapitalAllocator
from database.manager import HybridDatabaseManager

@pytest.mark.asyncio
async def test_concurrent_allocation_prevents_over_leverage():
    db  = HybridDatabaseManager()
    await db.init_db()
    alloc = SmartCapitalAllocator(1_000_000, {"WEEKLY": 0.5}, db)
    # two coroutines race for 600k each (total 1.2M > 500k limit)
    tasks = [
        alloc.allocate_capital("WEEKLY", 600_000, "T1"),
        alloc.allocate_capital("WEEKLY", 600_000, "T2")
    ]
    results = await asyncio.gather(*tasks)
    assert sum(results) == 1  # exactly one succeeds
