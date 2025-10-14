import json
from typing import Any, Dict
from starlette.responses import Response
import os
import pytest
from fastapi.testclient import TestClient
from app.main import app
from dotenv import load_dotenv
from pathlib import Path

# read .env from repo root
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ENV_PATH, override=False)

@pytest.fixture()
def client():
    os.environ.setdefault("PORT", "8000")
    return TestClient(app)

# ----------------------- helpers -----------------------

class MockES:
    """Minimal ES stub the router expects."""
    def __init__(self, *, get_ret=None, search_ret=None, get_exc=None, search_exc=None):
        self._get_ret = get_ret
        self._search_ret = search_ret
        self._get_exc = get_exc
        self._search_exc = search_exc

    def get(self, **kwargs):
        if self._get_exc:
            raise self._get_exc
        return self._get_ret or {"found": False}

    def search(self, **kwargs):
        if self._search_exc:
            raise self._search_exc
        # mimic ES client (python) returning a dict-like object
        return self._search_ret or {"hits": {"hits": []}}


# ----------------------- /_search -----------------------

def test_search_population_delegates_to_run_search(client, monkeypatch):
    captured = {}

    def mock_run_search(index, body, **kwargs):
        captured.update(dict(index=index, body=body, kwargs=kwargs))
        return {
            "took": 1,
            "timed_out": False,
            "hits": {"total": 0, "max_score": 0.0, "hits": []},
            "aggregations": {},
        }

    import app.api.routers.population as r_pop
    import app.core.config as cfg

    monkeypatch.setattr(r_pop, "run_search", mock_run_search, raising=True)

    req = {"query": {"multi_match": {"query": "AFR", "fields": ["name"]}}, "size": 5}
    resp = client.post("/beta/population/_search", json=req)
    
    assert resp.status_code == 200
    assert captured["index"] == cfg.settings.INDEX_POPULATION
    assert captured["body"]["query"]["multi_match"]["query"] == "AFR"
    assert captured["kwargs"]["size_cap"] == cfg.settings.ES_ALL_SIZE_CAP


# ----------------------- GET /{pid} -----------------------

def test_get_population_by_id_found_prunes_overlapping_populations(client, monkeypatch):
    # es.get returns found doc; overlappingPopulations is blank -> should be removed
    es = MockES(
        get_ret={
            "found": True,
            "_source": {
                "elasticId": "POP1",
                "name": "Pop One",
                "overlappingPopulations": [],  # should be pruned
            },
        }
    )
    monkeypatch.setattr("app.api.routers.population.es", es, raising=True)

    resp = client.get("/beta/population/POP1")
    assert resp.status_code == 200
    payload = resp.json()
    assert "_source" in payload
    src = payload["_source"]
    assert src["elasticId"] == "POP1"
    assert "overlappingPopulations" not in src  # pruned


def test_get_population_fallback_by_elasticId_keyword(client, monkeypatch):
    # es.get -> not found; es.search -> one hit
    es = MockES(
        get_ret={"found": False},
        search_ret={
            "hits": {
                "hits": [
                    {
                        "_source": {
                            "elasticId": "POP2",
                            "name": "Pop Two",
                            "overlappingPopulations": [],  # should be pruned
                        }
                    }
                ]
            }
        },
    )
    monkeypatch.setattr("app.api.routers.population.es", es, raising=True)

    resp = client.get("/beta/population/POP2")
    assert resp.status_code == 200
    src = resp.json()["_source"]
    assert src["elasticId"] == "POP2"
    assert "overlappingPopulations" not in src


def test_get_population_not_found(client, monkeypatch):
    es = MockES(get_ret={"found": False}, search_ret={"hits": {"hits": []}})
    monkeypatch.setattr("app.api.routers.population.es", es, raising=True)

    resp = client.get("/beta/population/UNKNOWN")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Population not found"


def test_get_population_es_error_surfaces_as_502(client, monkeypatch):
    es = MockES(get_exc=RuntimeError("boom"))
    monkeypatch.setattr("app.api.routers.population.es", es, raising=True)

    resp = client.get("/beta/population/ANY")
    assert resp.status_code == 502
    # FastAPI’s detail contains our message prefix
    assert "Elasticsearch error" in resp.json()["detail"]


def test_get_population_keeps_non_empty_overlapping_populations(client, monkeypatch):
    es = MockES(
        get_ret={
            "found": True,
            "_source": {
                "elasticId": "POP3",
                "name": "Pop Three",
                "overlappingPopulations": [{"elasticId": "OTHER", "name": "Other"}],
            },
        }
    )
    monkeypatch.setattr("app.api.routers.population.es", es, raising=True)

    resp = client.get("/beta/population/POP3")
    assert resp.status_code == 200
    src = resp.json()["_source"]
    assert "overlappingPopulations" in src
    assert len(src["overlappingPopulations"]) == 1


# ----------------------- POST /_search/{filename}.tsv -----------------------

@pytest.mark.parametrize("json_body", [
    {"query": {"match_all": {}}, "size": 2},
    None,  # simulate form not provided -> export util should handle defaulting
])
def test_export_populations_tsv_calls_util_and_returns_response(client, monkeypatch, json_body):
    # capture arguments passed to export_tsv_response
    called = {}

    async def mock_export_tsv_response(**kwargs):
        called.update(kwargs)
        return Response(b"ok", media_type="text/tab-separated-values")

    monkeypatch.setattr(
        "app.api.routers.population.export_tsv_response",
        mock_export_tsv_response,
        raising=True,
    )

    data = {}
    if json_body is not None:
        data["json"] = json.dumps(json_body)

    resp = client.post("/beta/population/_search/demo.tsv", data=data)
    assert resp.status_code == 200
    assert resp.content == b"ok"
    assert resp.headers["content-type"].startswith("text/tab-separated-values")

    # sanity-check the key params passed down
    assert called["index"]  # should be the population index
    assert called["filename"] == "demo"
    assert "size_cap" in called
    assert isinstance(called["default_fields"], list)
    assert callable(called["rewrite"])