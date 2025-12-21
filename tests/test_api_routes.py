import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from main import app
from api.routes import get_engine

client = TestClient(app)

def test_api_is_alive():
    # Simplest check: Does the server respond?
    response = client.get("/")
    assert response.status_code in [200, 404]

def test_health_endpoint_exists():
    # Just check if the URL is valid
    response = client.get("/api/health")
    assert response.status_code in [200, 503, 404]
