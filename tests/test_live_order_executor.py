import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from trading.live_order_executor import LiveOrderExecutor
from core.models import Position, MultiLegTrade, TradeStatus, StrategyType, CapitalBucket, ExpiryType, GreeksSnapshot

@pytest.fixture
def mock_api():
    api = AsyncMock()
    api.get_market_quote_ohlc = AsyncMock(return_value={"status": "success", "data": {}})
    api.place_multi_order = AsyncMock()
    return api

@pytest.fixture
def executor(mock_api):
    return LiveOrderExecutor(mock_api, AsyncMock())

@pytest.mark.asyncio
async def test_slice_quantity_shares_vs_contracts(executor):
    # This is the most important test for NIFTY freeze limits
    huge_qty = 7500 
    slices = executor._slice_quantity(huge_qty)
    assert sum(slices) == 7500
    assert max(slices) == 1800 

@pytest.mark.asyncio
async def test_idempotent_order_ids(executor):
    cid1 = executor._client_order_id("T-123", "HEDGE", 0, 0)
    cid2 = executor._client_order_id("T-123", "HEDGE", 0, 0)
    assert cid1 == cid2
