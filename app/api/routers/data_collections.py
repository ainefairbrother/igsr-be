# app/api/routers/data_collections.py
"""
Data Collections router
=======================
FE path: /api/beta/data-collection/*  ->  here: /beta/data-collection/*
"""

from fastapi import APIRouter, Body
from typing import Any, Dict, Optional
from app.core.config import settings
from app.lib.search_utils import run_search

router = APIRouter(prefix="/beta/data-collection", tags=["data-collection"])
INDEX = settings.INDEX_DATA_COLLECTIONS

# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def search_data_collections(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
    )