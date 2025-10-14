import pytest
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture()
def client():
    return TestClient(app)

def test_cors_preflight(client):
    r = client.options(
        "/beta/sample/_search",
        headers={
            "Origin": "http://localhost:8080",
            "Access-Control-Request-Method": "POST",
        }
    )
    assert r.status_code in (200, 204)
    assert r.headers.get("access-control-allow-origin") == "http://localhost:8080"