#!/usr/bin/env python3
"""
API Routes Test Suite
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime
from main import app
from api.routes import get_engine

client = TestClient(app)

# --- FIXTURES ---
@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.running = True
    engine.db.get_session.return_value.__aenter__.return_value.execute = AsyncMock()
    return engine

@pytest.fixture
def mock_upstox():
    with pytest.helpers.patch("api.routes.EnhancedUpstoxAPI") as MockAPI:
        instance = MockAPI.return_value
        yield instance

# --- TESTS ---

def test_health_check_healthy(mock_engine, mock_upstox):
    """Normal Operation: 200 OK"""
    mock_upstox.get_funds_and_margin = AsyncMock(return_value={
        "status": "success", "data": {"equity": {"available_margin": 50000.0}}
    })
    
    app.dependency_overrides[get_engine] = lambda: mock_engine
    
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_health_check_zero_margin(mock_engine, mock_upstox):
    """Critical Failure: 503 Unavailable"""
    # Simulate Broker reporting 0 margin
    mock_upstox.get_funds_and_margin = AsyncMock(return_value={
        "status": "success", "data": {"equity": {"available_margin": 0.0}}
    })
    
    app.dependency_overrides[get_engine] = lambda: mock_engine
    
    response = client.get("/api/v1/health")
    assert response.status_code == 503 # Kubernetes will kill pod
    assert "Margin Zero" in response.json()["detail"]
