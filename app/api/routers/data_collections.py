"""
Data Collections router
=======================
"""

from fastapi import APIRouter, Body
from typing import Any, Dict, Optional
from app.core.config import settings
from app.lib.search_utils import run_search
from app.lib.es_utils import (
    gate_short_text,
    rewrite_match_queries, 
    rewrite_terms_for_data_collection, 
    compose_rewrites
)

router = APIRouter(prefix="/beta/data-collection", tags=["data-collection"])
INDEX = settings.INDEX_DATA_COLLECTIONS

# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def search_data_collections(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(
            gate_short_text(2), 
            rewrite_match_queries, 
            rewrite_terms_for_data_collection
        )
    )