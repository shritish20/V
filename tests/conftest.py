# tests/conftest.py
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from core.config import settings

# 1. Force Test Environment
settings.SAFETY_MODE = "paper"
settings.UPSTOX_ACCESS_TOKEN = "TEST_TOKEN"
settings.POSTGRES_DB = "test_db"
# Ensure we don't accidentally connect to prod DB
settings.DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_db():
    """Mocks the HybridDatabaseManager to prevent real DB connections."""
    db = MagicMock()
    session = AsyncMock()
    
    # Mock context manager: async with db.get_session() as session:
    db.get_session.return_value.__aenter__.return_value = session
    db.get_session.return_value.__aexit__.return_value = None
    
    # Mock execute/scalars/first/all for SQLAlchemy queries
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = None
    mock_result.scalars.return_value.all.return_value = []
    session.execute.return_value = mock_result
    
    return db

@pytest.fixture
def mock_upstox():
    """Mocks EnhancedUpstoxAPI with Strict Schema Responses."""
    api = AsyncMock()
    
    # 1. Funds & Margin (Critical for Sheriff)
    api.get_funds_and_margin.return_value = {
        "status": "success",
        "data": {
            "equity": {
                "used_margin": 50000.0,
                "payin": 0.0,
                "available_margin": 1950000.0,
                "span_margin": 45000.0,
                "exposure_margin": 5000.0
            }
        }
    }

    # 2. Positions (Critical for Zombie Check)
    api.get_short_term_positions.return_value = [
        {
            "instrument_token": "NSE_FO|12345",
            "quantity": 75,
            "product": "I",
            "last_price": 100.0,
            "buy_price": 90.0,
            "pnl": 750.0,
            "trading_symbol": "NIFTY24JAN21500CE"
        }
    ]

    # 3. Order Placement
    api.place_order.return_value = {
        "status": "success",
        "data": {"order_id": "24010100056789"}
    }
    
    # 4. Token Check
    api.check_token_validity.return_value = True

    return api
