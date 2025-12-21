import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from trading.live_order_executor import LiveOrderExecutor
from core.models import Position, MultiLegTrade, TradeStatus, StrategyType, CapitalBucket, ExpiryType, GreeksSnapshot

@pytest.fixture
def mock_api():
    api = AsyncMock()
    api.get_market_quote_ohlc = AsyncMock()
    api.place_multi_order = AsyncMock()
    return api

@pytest.fixture
def mock_om():
    return AsyncMock()

@pytest.fixture
def executor(mock_api, mock_om):
    return LiveOrderExecutor(mock_api, mock_om)

@pytest.fixture
def fake_trade():
    dummy_greeks = GreeksSnapshot(delta=0.0, gamma=0.0, theta=0.0, vega=0.0, iv=0.0, updated_at=datetime.now())
    legs = [
        Position(
            symbol="NIFTY", instrument_key="OPT-NIFTY-CE", strike=22000,
            option_type="CE", quantity=75, entry_price=0.0, entry_time=datetime.now(),
            current_price=0.0, current_greeks=dummy_greeks, expiry_type=ExpiryType.WEEKLY,
            capital_bucket=CapitalBucket.WEEKLY
        )
    ]
    # Note: Using StrategyType.LONG_STRADDLE or whatever matches your enum
    return MultiLegTrade(
        legs=legs, strategy_type=list(StrategyType)[0], net_premium_per_share=0.0,
        entry_time=datetime.now(), expiry_date="2025-12-28", expiry_type=ExpiryType.WEEKLY,
        capital_bucket=CapitalBucket.WEEKLY, status=TradeStatus.PENDING, id="T-TEST-1"
    )

@pytest.mark.asyncio
async def test_slice_quantity_shares_vs_contracts(executor):
    huge_qty = 7500 # 100 Lots
    slices = executor._slice_quantity(huge_qty)
    assert sum(slices) == 7500
    assert max(slices) == 1800 

@pytest.mark.asyncio
async def test_idempotent_order_ids(executor):
    cid1 = executor._client_order_id("T-123", "HEDGE", 0, 0)
    cid2 = executor._client_order_id("T-123", "HEDGE", 0, 0)
    assert cid1 == cid2

@pytest.mark.asyncio
async def test_rollback_trigger(executor, mock_api, fake_trade):
    mock_api.get_market_quote_ohlc.return_value = {"status": "success", "data": {}}
    mock_api.place_multi_order.side_effect = [
        {"status": "success", "data": [{"order_id": "H1"}]},
        {"status": "error", "message": "Margin Blocked"},
        {"status": "success", "data": [{"order_id": "RB1"}]}
    ]
    ok, msg = await executor.execute_with_hedge_priority(fake_trade)
    assert ok is False
