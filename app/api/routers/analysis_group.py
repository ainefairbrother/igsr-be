"""
Analysis Group router
=====================
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, Body

from app.core.config import settings
from app.lib.search_utils import run_search

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


@router.post("/_search")
def search_analysis_group(
    body: Optional[Dict[str, Any]] = Body(None),
) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        postprocess=_apply_fe_label,
    )
