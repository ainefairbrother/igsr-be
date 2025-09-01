# app/api/routers/data_collections.py
#
# - Exposes the endpoints the Angular FE calls for *data collections*.
# - The FE hits:      POST /api/beta/data-collection/_search
# - Nginx strips /api and proxies to our app, so we register:  /beta/data-collection/_search
# - We forward the FE's request body to Elasticsearch, then "normalize" the ES
#   response so it exactly matches what the legacy FE expects (notably: numeric
#   `hits.total` and always-present `max_score`).
# - The FE sometimes sends `size: -1` to mean "return all". ES rejects negative
#   sizes, so we translate -1 to a large positive number (1000) that safely
#   covers our current DC count.

from fastapi import APIRouter, HTTPException, Request  # Request is imported for parity with other routers; not used here
from typing import Any, Dict, Optional

from app.services.es import es
from app.core.config import settings

# All routes in this module will be mounted under this prefix.
# Nginx is configured so that FE calls `/api/beta/data-collection/_search`,
# and that becomes `/beta/data-collection/_search` here.
router = APIRouter(prefix="/beta/data-collection", tags=["data-collections"])

# Which ES index to search for this resource type
# (configurable via .env; default is "data_collections").
INDEX = settings.INDEX_DATA_COLLECTIONS  # "data_collections"


def _normalize_es9(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make the ES 8/9 response look like what our Angular FE is coded against.

    Key adjustments:
    - ES >= 7 returns hits.total as an object: {"value": N, "relation": "eq|gte"}.
      The FE expects a plain number, so we coerce that to an int.
    - Some queries return max_score = null; FE code can assume it's a number.
      We set it to 0.0 if ES returned null/None.
    - Always return "aggregations" field (empty object if none), so FE templates
      don't need to null-check before accessing it.
    """
    # Copy through standard ES fields, defaulting to safe values
    took = resp.get("took", 0)
    timed_out = resp.get("timed_out", False)

    # Normalize the "hits" block
    hits = resp.get("hits", {}) or {}
    total = hits.get("total", 0)
    if isinstance(total, dict):
        # ES 7+/8+/9+ style: {"value": 123, "relation": "..."}
        total = total.get("value", 0)
    hits["total"] = total

    # Ensure max_score is always a number (FE sometimes treats it as numeric)
    if hits.get("max_score") is None:
        hits["max_score"] = 0.0

    # Always include "aggregations" (empty object is fine)
    aggs = resp.get("aggregations", {}) or {}

    # Return the minimal shape the FE expects
    return {"took": took, "timed_out": timed_out, "hits": hits, "aggregations": aggs}


@router.post("/_search")
def search_data_collections(body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    POST /beta/data-collection/_search

    Pass-through search to Elasticsearch with a couple of compatibility tweaks:

    1) size:-1 → 1000
       The legacy FE uses size:-1 to mean "give me everything".
       ES rejects negative sizes, so we map any negative size to a large,
       safe default (1000). Adjust this if the DC count grows beyond that.

    2) Default sort
       If no sort is provided, we sort by 'displayOrder' ascending, which matches
       how the FE typically presents collections.

    NOTE: We keep the request body as-is otherwise (queries, filters, etc.) and
          we return a normalized ES response that preserves the fields the FE uses.
    """
    # If the FE didn't send a body, default to a match_all query.
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # Handle "all docs" convention from the FE (size:-1)
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = 1000  # big enough for current DC count; increase if needed

    # Provide a stable default sort if none is specified by the FE.
    if "sort" not in es_body:
        es_body["sort"] = [{"displayOrder": {"order": "asc"}}]

    # Call ES and map any errors to a 502 (bad gateway) for clarity in the FE.
    try:
        # ignore_unavailable=True → if the index doesn't exist yet, ES won't hard-fail
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        # Bubble a concise error up to the FE; logs will carry the stack trace.
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # Return the FE-shaped response
    return _normalize_es9(resp)


@router.get("/_search")
def search_data_collections_get() -> Dict[str, Any]:
    """
    GET /beta/data-collection/_search

    Convenience endpoint used when a browser or a simple link hits this path via GET.
    Mirrors a basic POST with match_all and a generous size, so you can test in a browser:
      http://localhost:8080/api/beta/data-collection/_search
    """
    return search_data_collections({"query": {"match_all": {}}, "size": 1000})