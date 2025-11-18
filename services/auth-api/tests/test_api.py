"""Tests for Auth API endpoints"""

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    """Create a test client"""
    return TestClient(app)


def test_root_endpoint(client):
    """Test the root endpoint returns health status"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["version"] == "1.0.0"


def test_health_endpoint(client):
    """Test the health endpoint"""
    response = client.get("/health")
    # May return 200 or 503 depending on database availability
    assert response.status_code in [200, 503]
    data = response.json()
    assert "status" in data
    assert "version" in data


def test_openapi_schema(client):
    """Test that OpenAPI schema is accessible"""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert "openapi" in schema
    assert schema["info"]["title"] == "Auth API"
    assert schema["info"]["version"] == "1.0.0"


def test_docs_accessible(client):
    """Test that API documentation is accessible"""
    response = client.get("/docs")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_redoc_accessible(client):
    """Test that ReDoc documentation is accessible"""
    response = client.get("/redoc")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
