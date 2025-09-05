# app/api/routers/data_collections.py
"""
Data Collections router
=======================
FE path: /api/beta/data-collection/*  →  here: /beta/data-collection/*

Description
-----------
Pass-through to Elasticsearch for the Data Collections list with a couple of
compatibility tweaks so the legacy FE continues to work regardless of index shape

- the FE sometimes sends `size:-1` to mean *return all*  
  we map any negative `size` to a safe cap: `settings.ES_ALL_SIZE_CAP`
- if no sort is provided we default to `displayOrder` ascending  
  this matches how the FE presents collections
- the ES response is normalised via `normalise_es_response` so the FE gets the
  legacy shape it expects, including numeric `hits.total` and a present `max_score`
"""

from fastapi import APIRouter, HTTPException, Request
from typing import Any, Dict, Optional
from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

# FE calls /api/beta/data-collection/_search and nginx rewrites it to /beta/data-collection/_search here
router = APIRouter(prefix="/beta/data-collection", tags=["data-collections"])

# which ES index or alias to query
INDEX = settings.INDEX_DATA_COLLECTIONS


# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def search_data_collections(body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    POST /beta/data-collection/_search

    Forward the FE body to Elasticsearch with small adjustments so requests are valid
    and results are predictable for the UI
      - negative `size` (FE uses -1 for *all*) is mapped to `settings.ES_ALL_SIZE_CAP`
      - if no `sort` is present we sort by `displayOrder` ascending

    The ES response is normalised to the FE’s legacy shape
    """
    # default to match_all if the FE did not send a body
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # handle *return all* requested by the FE with size:-1
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # provide a stable default sort if the FE omitted one
    if "sort" not in es_body:
        es_body["sort"] = [{"displayOrder": {"order": "asc"}}]

    # call ES and translate failures to 502 for the FE
    try:
        # ignore_unavailable=True so a missing index returns an empty page rather than an error
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # return response in the legacy FE shape
    return normalise_es_response(resp)


# for dev
# curl -s -XGET http://localhost:8080/api/beta/data-collection/_search | jq
@router.get("/_search")
def search_data_collections_get() -> Dict[str, Any]:
    """
    GET /beta/data-collection/_search

    Convenience endpoint that mirrors a basic POST with match_all and a generous size
    useful for spot checks in a browser
    """
    return search_data_collections({"query": {"match_all": {}}, "size": 1000})