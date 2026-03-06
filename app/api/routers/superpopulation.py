"""
Superpopulation router
=======================
"""

from fastapi import APIRouter, Body
from typing import Any, Dict, Optional
from app.core.config import settings
from app.lib.search_utils import run_search
from app.api.schemas import SearchResponse, ErrorDetailResponse, SearchRequest

router = APIRouter(prefix="/beta/superpopulation", tags=["Superpopulation"])
INDEX = settings.INDEX_SUPERPOPULATION

# ------------------------------ Endpoints ------------------------------------


@router.post(
    "/_search",
    summary="Find superpopulations",
    description=(
        "Browse or filter superpopulation groups. "
        "Returns matching records and the total number of matches."
    ),
    response_model=SearchResponse,
    response_description="A list of matching superpopulations, plus the total number of matches.",
    responses={
        502: {
            "model": ErrorDetailResponse,
            "description": (
                "Search is temporarily unavailable because the backend cannot reach "
                "the search service."
            ),
            "content": {
                "application/json": {"example": {"detail": "backend_unavailable"}}
            },
        }
    },
)
def search_superpopulation(
    body: Optional[SearchRequest] = Body(
        None,
        example={"query": {"match_all": {}}, "size": 25},
        description=(
            "Search filters and options. If size is -1, the API returns as many results as allowed by the server limit."
        ),
    ),
) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body.model_dump(by_alias=True, exclude_none=True) if body else None,
        size_cap=settings.ES_ALL_SIZE_CAP,
    )
