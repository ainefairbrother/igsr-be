"""
Samples router
==============
"""

from fastapi import APIRouter, HTTPException, Response, Form, Body, Path, Request
from typing import Any, Dict, Optional

from app.services.es import es
from app.core.config import settings
from app.lib.search_utils import run_search
from app.lib.dl_utils import export_tsv_response
from app.lib.es_utils import (
    gate_short_text,
    rewrite_terms_for_samples,
    rewrite_match_queries,
    compose_rewrites,
    prune_empty_fields,
)

router = APIRouter(prefix="/beta/sample", tags=["samples"])
INDEX = settings.INDEX_SAMPLE

# ------------------------------ Endpoints ------------------------------------


@router.post("/_search")
def search_samples(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/sample/_search
    """
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(
            gate_short_text(2), rewrite_terms_for_samples, rewrite_match_queries
        ),
    )


@router.get("/{name}")
def get_sample(
    name: str = Path(..., description="Sample identifier (often the ES _id)"),
) -> Dict[str, Any]:
    """
    GET /beta/sample/{name}

    Response shape matches FE expectations: { "_source": { ...Sample... } }
    """
    # try by ES _id first for speed
    try:
        doc = es.get(index=INDEX, id=name, ignore=[404])
        if doc and doc.get("found"):
            src = doc.get("_source", {}) or {}
            if isinstance(src, dict):
                prune_empty_fields(
                    src, keys=("sharedSamples",)
                )  # removal means FE won't render empty box
            return {"_source": src}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # fallback: search by unique name.keyword
    try:
        resp = es.search(
            index=INDEX,
            body={
                "size": 1,
                "query": {"term": {"name.keyword": name}},
                "_source": True,
            },
            ignore_unavailable=True,
        )
        hit = (resp.get("hits", {}) or {}).get("hits", [])
        if hit:
            src = hit[0].get("_source", {}) or {}
            if isinstance(src, dict):
                prune_empty_fields(src, keys=("sharedSamples",))
            return {"_source": src}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    raise HTTPException(status_code=404, detail="Sample not found")


@router.post("/_search/{filename}.tsv")
async def export_samples_tsv(
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
        default_fields=["_id", "name", "sex"],
        rewrite=rewrite_terms_for_samples,
    )
