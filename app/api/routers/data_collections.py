# app/api/routers/data_collections.py
#
# Description:
# - Exposes the endpoints the FE calls for "data collections"
# - The FE calls POST /api/beta/data-collection/_search
# - Nginx strips /api to make:/beta/data-collection/_search
# - FE's request body is forwarded to Elasticsearch, then the ES response is
#   normalised so that it exactly matches what the legacy FE expects (notably: numeric
#   `hits.total` and always-present `max_score`)
# - The FE sometimes sends `size: -1` to mean "return all", but ES rejects negative
#   sizes, so -1 is translated to a large positive number (1000) that safely
#   covers the current DC count (as of 2025, we have 18 data collections).

from fastapi import APIRouter, HTTPException, Request
from typing import Any, Dict, Optional
from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

# Nginx is configured so that FE calls `/api/beta/data-collection/_search`,
# and that becomes `/beta/data-collection/_search` here
router = APIRouter(prefix="/beta/data-collection", tags=["data-collections"])

# Which ES index to search (configurable via .env)
INDEX = settings.INDEX_DATA_COLLECTIONS  # "data_collections"

# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def search_data_collections(body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    POST /beta/data-collection/_search

    FE call to Elasticsearch for data collections, with some small
    adjustments to the request and response to match FE expectations:
    - The FE uses size:-1, meaning "return all". ES rejects negative sizes, 
      so any negative size is mapped to a large, safe default (10,000, set in core/config.py)
    - If no sort is provided, sorts by 'displayOrder' ascending, which matches
      how the FE typically presents collections

    Other than these things, the request body is kept as-is and a normalised ES response 
    is returned, preserving the fields the FE uses
    """
    # If the FE didn't send a body, default to a match_all query
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # Handle "return all" from the FE (size:-1)
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # Provide a default sort if none is specified by the FE
    if "sort" not in es_body:
        es_body["sort"] = [{"displayOrder": {"order": "asc"}}]

    # Call ES and map any errors to a 502 (bad gateway, i.e. retrieval from ES didn't work)
    try:
        # ignore_unavailable=True - if the index doesn't exist yet, empty page, but no error
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # Return the FE-shaped response
    return normalise_es_response(resp)

# this is for dev
# to test, call with curl -s -XGET http://localhost:8080/api/beta/data-collection/_search | jq
@router.get("/_search")
def search_data_collections_get() -> Dict[str, Any]:
    """
    GET /beta/data-collection/_search

    Convenience endpoint used when a browser or a simple link hits this path via GET.
    Mirrors a basic POST with match_all and a generous size, so you can test in a browser:
      http://localhost:8080/api/beta/data-collection/_search
    """
    return search_data_collections({"query": {"match_all": {}}, "size": 1000})