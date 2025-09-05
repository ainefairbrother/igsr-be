# app/api/routers/population.py
"""
Population router
=================
FE path: /api/beta/population/*  →  here: /beta/population/*

Description
-----------
Queries Elasticsearch for the Populations list and detail with a small set of
compatibility adjustments so the current FE keeps working against the current index

- `size:-1` is capped to `settings.ES_ALL_SIZE_CAP`
- `track_total_hits=True` so totals are exact
- if no sort is provided we default to `superpopulation.display_order` ascending,
  then `name.keyword` ascending
- the FE sometimes filters on `dataCollections.title` or `dataCollections.title.std`
  inside `term/terms` queries, so rewrite those field names to
  `dataCollections.title.keyword` for exact matches

Nginx strips the `/api` prefix so FastAPI sees requests under `/beta/population/*`
All responses are normalised via `normalise_es_response` to match the legacy FE shape
"""

from fastapi import APIRouter, HTTPException, Body, Path
from typing import Any, Dict, Optional

from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

router = APIRouter(prefix="/beta/population", tags=["population"])

INDEX = settings.INDEX_POPULATION


# ------------------------------- Helpers ------------------------------------ #

def _rewrite_dc_title_to_keyword(node: Any) -> Any:
    """
    Map FE references to dataCollections.title(.std) → dataCollections.title.keyword
    inside term/terms bodies, other nodes pass through untouched
    """
    def _fix(field: str) -> str:
        return "dataCollections.title.keyword" if field in ("dataCollections.title", "dataCollections.title.std") else field

    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            if k in ("term", "terms") and isinstance(v, dict):
                out[k] = { _fix(f): _rewrite_dc_title_to_keyword(vv) for f, vv in v.items() }
            else:
                out[k] = _rewrite_dc_title_to_keyword(v)
        return out
    if isinstance(node, list):
        return [_rewrite_dc_title_to_keyword(x) for x in node]
    return node


# ------------------------------ Endpoints ------------------------------------ #

@router.post("/_search")
def search_population(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/population/_search

    Apply small FE compatibility tweaks then forward to ES
    """
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # handle “return all”
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # exact totals and stable default sort
    es_body.setdefault("track_total_hits", True)
    # if "sort" not in es_body:
    #     es_body["sort"] = [
    #         {"superpopulation.display_order": {"order": "asc"}},
    #         {"name.keyword": {"order": "asc"}},
    #     ]

    # FE 'data collection' filter: title.std → title.keyword
    es_body = _rewrite_dc_title_to_keyword(es_body)

    # query ES
    try:
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    return normalise_es_response(resp)


# for dev
@router.get("/_search")
def search_population_get() -> Dict[str, Any]:
    return search_population({"query": {"match_all": {}}, "size": 1000})


@router.get("/{pid}")
def get_population(pid: str = Path(..., description="Population identifier (ES _id or elasticId)")) -> Dict[str, Any]:
    """
    GET /beta/population/{pid}

    Response shape matches FE expectations: { "_source": { ...Population... } }
    """
    # try by ES _id first
    try:
        doc = es.get(index=INDEX, id=pid, ignore=[404])
        if doc and doc.get("found"):
            return {"_source": doc.get("_source", {})}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # fallback: by elasticId.keyword
    try:
        resp = es.search(
            index=INDEX,
            body={"size": 1, "query": {"term": {"elasticId.keyword": pid}}, "_source": True},
            ignore_unavailable=True,
        )
        hit = (resp.get("hits", {}) or {}).get("hits", [])
        if hit:
            return {"_source": hit[0].get("_source", {})}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    raise HTTPException(status_code=404, detail="Population not found")