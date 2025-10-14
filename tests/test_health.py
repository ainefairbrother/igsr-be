import pytest
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture()
def client():
    return TestClient(app)

def test_root_and_headers(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert r.headers.get("x-igsr-api") == "Python FastAPI"
    assert r.headers.get("x-igsr-api-version") == "2025"
