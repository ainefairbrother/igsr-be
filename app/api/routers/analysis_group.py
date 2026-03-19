"""
Analysis Group router
=====================
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, Body

from app.core.config import settings
from app.lib.search_utils import run_search
from app.api.schemas import SearchResponse, ErrorDetailResponse, SearchRequest

router = APIRouter(prefix="/beta/analysis-group", tags=["Analysis group"])
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


def _apply_fe_label(resp: Dict[str, Any], _es_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inject human-friendly labels for the FE into each hit.
    """
    hits = (resp.get("hits") or {}).get("hits") or []
    for h in hits:
        src = h.get("_source") or {}
        human = _choose_human_label(src)
        src["shortTitle"] = human
        if not src.get("title"):
            src["title"] = human
        h["_source"] = src
    return resp


# -------------------------------- Endpoints ------------------------------------ #


@router.post(
    "/_search",
    summary="List all analysis groups.",
    description=(
        "Get all analysis groups used across IGSR data. Response includes IDs and descriptions."
    ),
    response_model=SearchResponse,
    response_description="A list of analysis groups, plus the total number of matches.",
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
def search_analysis_group(
    body: Optional[SearchRequest] = Body(
        None,
        example={
            "query": {"match_all": {}},
            "size": 25,
            "sort": [{"displayOrder": "asc"}],
        },
        description=(
            "Search filters and options. If size is -1, the API returns as many results as allowed by the server limit."
        ),
    ),
) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body.model_dump(by_alias=True, exclude_none=True) if body else None,
        size_cap=settings.ES_ALL_SIZE_CAP,
        postprocess=_apply_fe_label,
    )
