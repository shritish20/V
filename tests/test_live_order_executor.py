import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from main import app
from api.routes import get_engine

client = TestClient(app)

@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.running = True
    return engine

def test_health_check_healthy(mock_engine):
    with patch("api.routes.EnhancedUpstoxAPI") as MockAPI:
        api_instance = MockAPI.return_value
        api_instance.get_funds_and_margin = AsyncMock(return_value={
            "status": "success", "data": {"equity": {"available_margin": 50000.0}}
        })
        app.dependency_overrides[get_engine] = lambda: mock_engine
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

def test_health_check_zero_margin(mock_engine):
    with patch("api.routes.EnhancedUpstoxAPI") as MockAPI:
        api_instance = MockAPI.return_value
        api_instance.get_funds_and_margin = AsyncMock(return_value={
            "status": "success", "data": {"equity": {"available_margin": 0.0}}
        })
        app.dependency_overrides[get_engine] = lambda: mock_engine
        response = client.get("/api/v1/health")
        # System should return 503 if margin is zero
        assert response.status_code == 503
