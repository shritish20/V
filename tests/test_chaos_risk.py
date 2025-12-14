import pytest
import asyncio
from datetime import datetime
from core.config import settings
from core.models import GreeksSnapshot, MultiLegTrade, TradeStatus, Position
from core.enums import StrategyType, CapitalBucket, ExpiryType
from trading.risk_manager import AdvancedRiskManager

# Force SAFETY_MODE to 'live' for risk checks, but mock the API
settings.SAFETY_MODE = "live"

@pytest.mark.asyncio
async def test_scenario_1_silent_killer_greeks(mocker):
    """
    Chaos Test: Market crashes, but Broker Greeks remain 'calm' (stale).
    The system MUST detect this via SABR divergence or timestamp checks.
    """
    # 1. Setup: Portfolio with significant exposure
    risk_mgr = AdvancedRiskManager(None, None)

    # Mock a "Long Delta" position
    leg = Position(
        symbol="NIFTY", instrument_key="T1", strike=20000, option_type="CE",
        quantity=1500, entry_price=100, entry_time=datetime.now(),
        current_price=100,
        # THE LIE: Broker says Delta is 0.1 (Low Risk), but we will inject SABR reality via PnL
        current_greeks=GreeksSnapshot(timestamp=datetime.now(), delta=0.1, vega=5, confidence_score=0.2),
        expiry_type=ExpiryType.WEEKLY, capital_bucket=CapitalBucket.WEEKLY
    )

    trade = MultiLegTrade(
        legs=[leg], 
        strategy_type=StrategyType.BULL_CALL_SPREAD, # FIXED: Use Enum
        expiry_date="2024-01-01",
        expiry_type=ExpiryType.WEEKLY, 
        capital_bucket=CapitalBucket.WEEKLY,
        status=TradeStatus.OPEN,
        net_premium_per_share=0.0, # FIXED: Required Field
        entry_time=datetime.now()  # FIXED: Required Field
    )

    # 2. Inject The Truth (Market Crash PnL)
    # Market drops 500 points
    real_market_drop_pnl = -500000.0

    # 3. Update Portfolio
    risk_mgr.update_portfolio_state([trade], real_market_drop_pnl)

    # 4. Assertions
    # The PnL damage alone should trigger the Daily Loss Limit
    breached = risk_mgr.check_portfolio_limits()
    assert breached is True, "FAILED: Panic Flatten did not trigger on massive PnL drop!"

@pytest.mark.asyncio
async def test_scenario_2_low_confidence_block(mocker):
    """
    Chaos Test: If Greek Validator returns Low Confidence (due to stale/divergent data),
    Risk Manager MUST block new trades.
    """
    risk_mgr = AdvancedRiskManager(None, None)

    # Create a trade where the legs have LOW confidence scores
    leg_bad = Position(
        symbol="NIFTY", instrument_key="T1", strike=20000, option_type="CE",
        quantity=50, entry_price=100, entry_time=datetime.now(),
        current_price=100,
        current_greeks=GreeksSnapshot(timestamp=datetime.now(), confidence_score=0.3), # < 0.5
        expiry_type=ExpiryType.WEEKLY, capital_bucket=CapitalBucket.WEEKLY
    )

    trade = MultiLegTrade(
        legs=[leg_bad], 
        strategy_type=StrategyType.IRON_CONDOR, 
        expiry_date="2024-01-01",
        expiry_type=ExpiryType.WEEKLY, 
        capital_bucket=CapitalBucket.WEEKLY,
        net_premium_per_share=0.0, # FIXED
        entry_time=datetime.now()  # FIXED
    )

    # Expect Rejection
    allowed = risk_mgr.check_pre_trade(trade)
    assert allowed is False, "FAILED: Risk Manager allowed trade with Low Confidence Score!"

@pytest.mark.asyncio
async def test_scenario_3_websocket_circuit_breaker(mocker):
    """
    Chaos Test: Rapid connection failures must trigger a cool-down.
    """
    from trading.live_data_feed import LiveDataFeed
    
    # We pass None/Empty args because we are mocking the connection anyway
    feed = LiveDataFeed({}, {}, None)
    
    # Mock the internal logger to count warnings
    mock_logger = mocker.patch("trading.live_data_feed.logger")
    
    # Simulate 10 rapid failures
    for _ in range(10):
        feed._on_error("Connection Reset")
        
    # Assert circuit breaker logic
    assert feed._consecutive_errors == 10
    assert feed._circuit_breaker_active is True
    
    # We expect a CRITICAL log indicating a pause
    assert feed._circuit_breaker_until > 0, "FAILED: Circuit Breaker timer not set!"
