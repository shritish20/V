import pytest
import asyncio
from datetime import datetime
from unittest.mock import MagicMock
from core.config import settings

# Force TEST mode config
settings.SAFETY_MODE = "paper"
settings.ACCOUNT_SIZE = 1000000.0

@pytest.fixture
def mock_api():
    api = MagicMock()
    # Mock successful batch response
    api.place_multi_order.return_value = {
        "status": "success",
        "data": [{"order_id": "123", "correlation_id": "T1-LEG0"}]
    }
    return api

@pytest.fixture
def mock_db():
    db = MagicMock()
    return db

@pytest.fixture
def mock_pricing():
    pricing = MagicMock()
    # Return dummy greeks
    pricing.calculate_greeks.return_value = MagicMock(
        delta=0.5, gamma=0.01, theta=-10.0, vega=5.0, iv=0.15
    )
    return pricing
