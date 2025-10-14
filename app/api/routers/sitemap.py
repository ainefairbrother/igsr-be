"""
Sitemap router
=======================
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, Body

from app.core.config import settings
from app.lib.search_utils import run_search
from app.lib.es_utils import gate_short_text, rewrite_match_queries, compose_rewrites

router = APIRouter(prefix="/sitemap", tags=["sitemap"])
INDEX = settings.INDEX_SITEMAP

# ------------------------------ Endpoints ------------------------------------


@router.post("/_search")
def search_sitemap(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/sitemap/_search
    """
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(gate_short_text(2), rewrite_match_queries),
    )
