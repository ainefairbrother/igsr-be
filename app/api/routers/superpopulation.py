"""
Superpopulation router
=======================
"""

from fastapi import APIRouter, Body
from typing import Any, Dict, Optional
from app.core.config import settings
from app.lib.search_utils import run_search

router = APIRouter(prefix="/beta/superpopulation", tags=["superpopulation"])
INDEX = settings.INDEX_SUPERPOPULATION

# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def search_superpopulation(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
    )