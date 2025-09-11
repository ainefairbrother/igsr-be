# app/api/routers/superpopulation.py
"""
Superpopulation router
=======================
FE path: /api/beta/superpopulation/*  →  here: /beta/superpopulation/*
"""

from fastapi import APIRouter, HTTPException, Request
from typing import Any, Dict, Optional
from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

router = APIRouter(prefix="/beta/superpopulation", tags=["superpopulation"])
INDEX = settings.INDEX_SUPERPOPULATION


# ------------------------------ Endpoints ------------------------------------


@router.post("/_search")
def search_superpopulation(body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    
    # default to match_all if the FE did not send a body
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # handle *return all* requested by the FE with size:-1
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # call ES and translate failures to 502 for the FE
    try:
        # ignore_unavailable=True so a missing index returns an empty page rather than an error
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # return response in the legacy FE shape
    return normalise_es_response(resp)

# for dev
# curl -s -XGET http://localhost:8080/api/beta/superpopulation/_search | jq
@router.get("/_search")
def search_superpopulation_get() -> Dict[str, Any]:
    return search_superpopulation({"query": {"match_all": {}}, "size": 1000})