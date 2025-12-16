"""
Superpopulation router
=======================
"""

from fastapi import APIRouter, Body
from typing import Any, Dict, Optional
from app.core.config import settings
from app.lib.search_utils import run_search
from app.api.schemas import SearchResponse

router = APIRouter(prefix="/beta/superpopulation", tags=["superpopulation"])
INDEX = settings.INDEX_SUPERPOPULATION

# ------------------------------ Endpoints ------------------------------------


@router.post(
    "/_search",
    summary="Search superpopulations",
    response_model=SearchResponse,
    response_description="Normalised Elasticsearch response for superpopulation search",
)
def search_superpopulation(
    body: Optional[Dict[str, Any]] = Body(
        None,
        example={"query": {"match_all": {}}, "size": 25},
        description="Elasticsearch search payload; size:-1 is capped server-side.",
    ),
) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
    )
