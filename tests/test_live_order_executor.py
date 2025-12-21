#!/usr/bin/env python3
"""
LiveOrderExecutor 20.1 – Production Test Suite
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from trading.live_order_executor import LiveOrderExecutor, RollbackFailure
from core.models import Position, MultiLegTrade, TradeStatus, StrategyType, CapitalBucket, ExpiryType
from core.config import settings

# --- FIXTURES ---
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
    legs = [
        Position(
            symbol="NIFTY", instrument_key="OPT-NIFTY-CE", strike=22000,
            option_type="CE", quantity=75, entry_price=0, entry_time=datetime.now(),
            current_price=0, current_greeks=None, expiry_type=ExpiryType.WEEKLY,
            capital_bucket=CapitalBucket.WEEKLY
        ),
        Position(
            symbol="NIFTY", instrument_key="OPT-NIFTY-PE", strike=22000,
            option_type="PE", quantity=-75, entry_price=0, entry_time=datetime.now(),
            current_price=0, current_greeks=None, expiry_type=ExpiryType.WEEKLY,
            capital_bucket=CapitalBucket.WEEKLY
        )
    ]
    return MultiLegTrade(
        legs=legs, strategy_type=StrategyType.STRADDLE, net_premium_per_share=0,
        entry_time=datetime.now(), expiry_date="2025-02-27", expiry_type=ExpiryType.WEEKLY,
        capital_bucket=CapitalBucket.WEEKLY, status=TradeStatus.PENDING, id="T-TEST-1"
    )

# --- TESTS ---

@pytest.mark.asyncio
async def test_slice_quantity_shares_vs_contracts(executor):
    """
    CRITICAL: Verify we slice by SHARES (1800) not CONTRACTS (24).
    If this fails, your config is wrong.
    """
    # 1. Setup a Huge Order: 100 Lots of Nifty (7500 Shares)
    # Freeze Limit in Config is 1800 SHARES.
    huge_qty = 100 * 75 # 7500
    
    # 2. Run Slicing
    slices = executor._slice_quantity(huge_qty)
    
    # 3. Assertions
    # We expect slices of 1800, 1800, 1800, 1800, 300
    assert sum(slices) == 7500
    assert max(slices) == 1800 
    assert len(slices) == 5 
    assert slices == [1800, 1800, 1800, 1800, 300]

@pytest.mark.asyncio
async def test_idempotent_order_ids(executor):
    """
    Verify that the same trade ID always generates the same 'correlation_id'.
    This prevents duplicate orders if the bot restarts.
    """
    cid1 = executor._client_order_id("T-123", "HEDGE", 0, 0)
    cid2 = executor._client_order_id("T-123", "HEDGE", 0, 0)
    
    assert cid1 == cid2
    assert cid1.startswith("VG")

@pytest.mark.asyncio
async def test_rollback_trigger(executor, mock_api, fake_trade):
    """
    If the Risk leg fails, the Hedge leg must be rolled back.
    """
    # Mock Quotes
    mock_api.get_market_quote_ohlc.return_value = {"status": "success", "data": {}}
    
    # Sequence: Hedge Success -> Risk Fail -> Rollback Success
    mock_api.place_multi_order.side_effect = [
        {"status": "success", "data": [{"order_id": "H1"}]}, # Hedge
        {"status": "error", "message": "Margin Blocked"},    # Risk
        {"status": "success", "data": [{"order_id": "RB1"}]} # Rollback
    ]

    ok, msg = await executor.execute_with_hedge_priority(fake_trade)
    
    assert ok is False
    assert "rolled back" in msg.lower()
    assert mock_api.place_multi_order.call_count == 3
