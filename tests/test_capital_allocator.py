import pytest
from unittest.mock import AsyncMock, MagicMock
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_allocator_initialization():
    # Verify the logic boots up
    alloc = SmartCapitalAllocator(2000000.0, {"intraday": 0.1}, MagicMock())
    assert alloc.fallback_account_size == 2000000.0
