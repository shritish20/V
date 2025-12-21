import pytest
from unittest.mock import MagicMock
from capital.allocator import SmartCapitalAllocator

@pytest.mark.asyncio
async def test_allocator_initialization():
    # We just want to ensure the class can be instantiated with its dependencies
    # and doesn't crash on startup.
    try:
        alloc = SmartCapitalAllocator(
            fallback_account_size=2000000.0, 
            allocation_config={"intraday": 0.1}, 
            db=MagicMock()
        )
        assert alloc is not None
    except Exception as e:
        pytest.fail(f"Allocator failed to initialize: {e}")
