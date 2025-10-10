# app/api/routers/population.py
"""
Population router
=================
FE path: /api/beta/population/*  ->  here: /beta/population/*
"""

from fastapi import APIRouter, HTTPException, Body, Path, Request, Form, Response
from typing import Any, Dict, Optional

from app.services.es import es
from app.core.config import settings
from app.lib.search_utils import run_search
from app.lib.dl_utils import export_tsv_response
from app.lib.es_utils import (
    gate_short_text,
    rewrite_terms_for_population, 
    rewrite_match_queries, 
    compose_rewrites,
    prune_empty_fields
)

router = APIRouter(prefix="/beta/population", tags=["population"])
INDEX = settings.INDEX_POPULATION

# ------------------------------ Endpoints ------------------------------------ #

@router.post("/_search")
def search_population(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/population/_search"""
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(
            gate_short_text(2),
            rewrite_terms_for_population, 
            rewrite_match_queries
            )
    )


@router.get("/{pid}")
def get_population(pid: str = Path(..., description="Population identifier (ES _id or elasticId)")) -> Dict[str, Any]:
    """
    GET /beta/population/{pid}

    Response shape matches FE expectations: { "_source": { ...Population... } }
    """
    try:
        doc = es.get(index=INDEX, id=pid, ignore=[404])
        if doc and doc.get("found"):
            src = doc.get("_source", {}) or {}
            if isinstance(src, dict):
                prune_empty_fields(src, keys=("overlappingPopulations",)) # removal means FE won't render empty box
            return {"_source": src}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    try:
        resp = es.search(
            index=INDEX,
            body={"size": 1, "query": {"term": {"elasticId.keyword": pid}}, "_source": True},
            ignore_unavailable=True,
        )
        hit = (resp.get("hits", {}) or {}).get("hits", [])
        if hit:
            src = hit[0].get("_source", {}) or {}
            if isinstance(src, dict):
                prune_empty_fields(src, keys=("overlappingPopulations",)) 
            return {"_source": src}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    raise HTTPException(status_code=404, detail="Population not found")


@router.post("/_search/{filename}.tsv")
async def export_populations_tsv(
    filename: str,
    request: Request,
    json_form: Optional[str] = Form(None, alias="json"),
) -> Response:
    return await export_tsv_response(
        request=request,
        json_form=json_form,
        index=INDEX,
        filename=filename,
        size_cap=settings.ES_EXPORT_SIZE_CAP,
        default_fields=["elasticId", "name", "superpopulation.name", "latitude", "longitude"],
        rewrite=rewrite_terms_for_population,
    )