# app/api/routers/analysis_group.py
"""
Analysis Group router
=====================
FE path: /api/beta/analysis-group/*  →  here: /beta/analysis-group/*

Description
-----------
Communicates with Elasticsearch with a few adjustments so the FE keeps 
working regardless of whether the analysis-group index uses the
'old' fields (`title`, `shortTitle`) or the 'new' ones (`description`, `code`):

- `size:-1` (meaning “all”) is mapped to a safe cap: `settings.ES_ALL_SIZE_CAP`.
- If no sort is provided, we default to `displayOrder` ascending, then
  `title.keyword` ascending.
- `track_total_hits=True` so the FE gets exact totals.
- The FE renders checkbox labels / table headers from
  `_source.shortTitle`. Newer docs often set `shortTitle` to a code like “ONT”
  and place the long, human label in `description`. Satisfy FE by
  ensuring `_source.shortTitle` contains a **human-readable** label, preferring:
  `description` → `title` → existing `shortTitle` → `code`. Also make sure
  `_source.title` is populated for templates that expect it.

The ES response is returned via `normalise_es_response` to match the legacy
shape the FE expects (numeric `hits.total`, non-null `max_score`, etc.).
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException

from app.core.config import settings
from app.lib.es_utils import normalise_es_response
from app.services.es import es

router = APIRouter(prefix="/beta/analysis-group", tags=["analysis-group"])

# Which ES index/alias to query
INDEX = settings.INDEX_ANALYSIS_GROUP

# Large value for missing displayOrder
_INT64_MAX = 2**63 - 1


# ------------------------------- Helpers ------------------------------------ #

def _choose_human_label(src: Dict[str, Any]) -> str:
    """
    Pick a human label to show in the FE. These labels are used in the filter
    checkboxes and the sample table headers.

    Preference order:
      1) description  (new index long label)
      2) title        (old index long label)
      3) shortTitle   (may be a short code like "ONT")
      4) code         (machine code; worst-case fallback)
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
    Normalisation for FE templates:

    - Ensure `_source.shortTitle` is a human label (see preference above).
    - Ensure `_source.title` exists (fall back to the same human label).
    - Ensure `_source.displayOrder` is truthy; nulls sort last.
    """
    for h in hits:
        src = h.get("_source") or {}

        human = _choose_human_label(src)

        # FE renders from shortTitle; make it human-readable
        src["shortTitle"] = human

        # Some older docs might be missing title, add default
        if not src.get("title"):
            src["title"] = human

        # Default sort if displayOrder is missing
        if src.get("displayOrder") is None:
            src["displayOrder"] = _INT64_MAX

        h["_source"] = src


# -------------------------------- Routes ------------------------------------ #

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

    # Stable default sort if the FE didn’t send one
    if "sort" not in es_body:
        es_body["sort"] = [
            {"displayOrder": {"order": "asc"}},
            {"title.keyword": {"order": "asc"}},
        ]

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
@router.get("/_search")
def search_analysis_group_get() -> Dict[str, Any]:
    return search_analysis_group({"query": {"match_all": {}}, "size": 100})