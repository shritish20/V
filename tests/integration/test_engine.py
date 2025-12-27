import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from core.engine import VolGuard20Engine, TradeStatus
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_engine_heartbeat_check_failure(mock_db, mock_upstox):
    """Verify Engine halts if Sheriff is dead."""
    engine = VolGuard20Engine()
    engine.db = mock_db
    engine.api = mock_upstox
    # Bypass heavy init
    engine.instruments_master.download_and_load = AsyncMock()
    engine.data_fetcher.load_all_data = AsyncMock()
    engine.om.start = AsyncMock()
    # Setup DB to return an OLD heartbeat (Dead Sheriff)
    old_time = datetime.utcnow() - timedelta(seconds=60)
    mock_row = MagicMock()
    mock_row.sheriff_heartbeat = old_time
    mock_row.kill_switch_active = False
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = mock_row
    mock_db.get_session.return_value.__aenter__.return_value.execute.return_value = mock_result
    # Run the check
    is_safe = await engine._check_safety_heartbeat()
    assert is_safe is False  # Should return False (Unsafe)

@pytest.mark.asyncio
async def test_continuous_reconciliation(mock_db, mock_upstox):
    """Verify Engine calls broker to find zombies."""
    engine = VolGuard20Engine()
    engine.api = mock_upstox
    engine.db = mock_db
    engine._trade_lock = MagicMock()
    engine._trade_lock.__aenter__ = AsyncMock()
    engine._trade_lock.__aexit__ = AsyncMock()
    # Mock InstrumentMaster so zombie adoption succeeds
    engine.instruments_master = MagicMock()
    engine.instruments_master.df = MagicMock()
    fake_row = MagicMock()
    fake_row.__getitem__.side_effect = lambda k: {
        "strike_price": 21000,
        "instrument_type": "CE",
        "underlying_symbol": "NIFTY",
        "instrument_key": "123"
    }.get(k)
    engine.instruments_master.df.__getitem__.return_value = fake_row
    engine.instruments_master.df.empty = False
    # Mock Broker returning 1 position
    mock_upstox.get_short_term_positions.return_value = [
        {"instrument_token": "123", "quantity": 50, "pnl": 0}
    ]
    # We have 0 trades internally
    engine.trades = []
    # Run Reconcile
    await engine._reconcile_broker_positions()
    # Engine should have adopted 1 Zombie
    assert len(engine.trades) == 1
    assert engine.trades[0].status == TradeStatus.EXTERNAL
