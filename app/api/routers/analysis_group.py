# app/api/routers/analysis_group.py
"""
Analysis Group router
=====================
FE path: /api/beta/analysis-group/*  →  here: /beta/analysis-group/*
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException

from app.core.config import settings
from app.lib.es_utils import normalise_es_response
from app.services.es import es

router = APIRouter(prefix="/beta/analysis-group", tags=["analysis-group"])
INDEX = settings.INDEX_ANALYSIS_GROUP


# ------------------------------- Helpers ------------------------------------ #


def _choose_human_label(src: Dict[str, Any]) -> str:
    """
    Pick a human label to show in the FE. These labels are used in the filter
    checkboxes and the sample table headers.
    """
    return (
        src.get("description")
        or src.get("title")
        or src.get("shortTitle")
        or src.get("code")
        or ""
    )


def _apply_fe_label(hits: List[Dict[str, Any]]) -> None:
    """
    Normalisation for FE templates that expect human-readable labels in
    `_source.shortTitle` and a present `_source.title`.
    """
    for h in hits:
        src = h.get("_source") or {}

        human = _choose_human_label(src)

        # FE renders from shortTitle; make it human-readable
        src["shortTitle"] = human

        # Some older docs might be missing title, add default
        if not src.get("title"):
            src["title"] = human

        h["_source"] = src


# -------------------------------- Endpoints ------------------------------------ #


@router.post("/_search")
def search_analysis_group(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/analysis-group/_search

    Pass the FE body to Elasticsearch with some adjustments:
      - Map `size:-1` to `settings.ES_ALL_SIZE_CAP`
      - Default sort: `displayOrder` asc, then `title.keyword` asc
      - `track_total_hits=True` for exact totals
      - Apply a compatibility shim so FE labels are human-readable

    Returns:
        Dict[str, Any]: Response in the legacy FE shape.
    """
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # Exact totals for pages and badges
    es_body.setdefault("track_total_hits", True)

    # Query Elasticsearch
    try:
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # Apply the FE label fixer so the UI shows the long labels for tick boxes and table headers
    hits = ((resp.get("hits") or {}).get("hits") or [])
    _apply_fe_label(hits)

    # Normalise to legacy response shape
    return normalise_es_response(resp)

# for dev
# curl -s -XGET http://localhost:8080/api/beta/analysis-group/_search | jq
@router.get("/_search")
def search_analysis_group_get() -> Dict[str, Any]:
    return search_analysis_group({"query": {"match_all": {}}, "size": 100})