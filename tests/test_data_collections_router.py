
import pytest
from fastapi.testclient import TestClient
from app.main import app
from dotenv import load_dotenv
from pathlib import Path
import os

# read .env from repo root
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ENV_PATH, override=False)

@pytest.fixture()
def client():
    os.environ.setdefault("PORT", "8000")
    return TestClient(app)

# ----------------------- /_search -----------------------

import app.api.routers.data_collections as r_dc
import app.core.config as cfg

# test whether the router sends the right query to ES via run_search
def test_query_sent_to_ES_via_run_search(client, monkeypatch):
    
    captured = {}
    def mock_run_search(index, body, **kwargs):
        captured.update(dict(index=index, body=body, kwargs=kwargs))
        return {"took":1,"timed_out":False,"hits":{"total":0,"max_score":0.0,"hits":[]}, "aggregations":{}}

    monkeypatch.setattr(r_dc, "run_search", mock_run_search, raising=True)
    req = {"query": {"multi_match": {"query":"MAGE","fields":["title","shortTitle"]}}, "size": 5}
    resp = client.post("/beta/data-collection/_search", json=req)

    assert resp.status_code == 200
    assert captured["index"] == cfg.settings.INDEX_DATA_COLLECTIONS
    assert captured["body"]["size"] == 5
    assert "query" in captured["body"]
    
# test whether the router returns the expected payload from run_search
def test_payload_received_from_run_search(client, monkeypatch):
    expected = {
        "took": 1,
        "timed_out": False,
        "hits": {"total": 1, "max_score": 1.0, "hits": [{"_source": {"title": "MAGE RNA-seq"}}]},
        "aggregations": {},
    }

    monkeypatch.setattr(r_dc, "run_search", lambda *a, **k: expected, raising=True)
    resp = client.post("/beta/data-collection/_search", json={"query": {"match_all": {}}})

    assert resp.status_code == 200
    assert resp.json() == expected