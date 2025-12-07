import pytest
import asyncio
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_concurrent_allocation():
    """
    Race Condition Stress Test:
    Try to allocate 2 trades of 600k each when Limit is 1M.
    Only ONE should succeed.
    """
    # 1. Setup: 1M Limit
    allocator = SmartCapitalAllocator(1000000.0, {"weekly": 1.0})
    
    # 2. Define Allocation Task
    async def try_alloc(name):
        return await allocator.allocate_capital("weekly", 600000.0, name)
    
    # 3. Fire simultaneously
    results = await asyncio.gather(
        try_alloc("trade1"),
        try_alloc("trade2")
    )
    
    # 4. Assert
    success_count = sum(results)
    assert success_count == 1 # Only one should fit
    print(f"✅ Race Condition Handled. Successes: {success_count}/2")

@pytest.mark.asyncio
async def test_negative_allocation():
    allocator = SmartCapitalAllocator(1000000.0, {"weekly": 1.0})
    res = await allocator.allocate_capital("weekly", -5000, "bad_trade")
    assert res is False
    print("✅ Negative Allocation Blocked")
