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
from app.api.schemas import (
    SearchResponse,
    SourceDocument,
    ErrorDetailResponse,
    SearchRequest,
)

router = APIRouter(prefix="/beta/sample", tags=["Sample"])
INDEX = settings.INDEX_SAMPLE

# ------------------------------ Endpoints ------------------------------------


@router.post(
    "/_search",
    summary="List all samples.",
    description=(
        "Get all samples across the IGSR data. Response includes: ID, synonyms, sex, related samples, related populations and related data collections."
    ),
    response_model=SearchResponse,
    response_description="A list of matching samples, plus the total number of matches.",
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
def search_samples(
    body: Optional[SearchRequest] = Body(
        None,
        example={
            "query": {"match_all": {}},
            "size": 25,
            "sort": [{"name.keyword": "asc"}],
        },
        description=(
            "Search filters and options. If size is -1, the API returns as many results as allowed by the server limit."
        ),
    )
) -> Dict[str, Any]:
    """
    POST /beta/sample/_search
    """
    return run_search(
        INDEX,
        body.model_dump(by_alias=True, exclude_none=True) if body else None,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(
            gate_short_text(2), rewrite_terms_for_samples, rewrite_match_queries
        ),
    )


@router.get(
    "/{name}",
    summary="Look up one sample by ID.",
    description=(
        "Look up a single sample by sample ID. Response includes: ID, synonyms, sex, related samples, related populations and related data collections."
    ),
    response_model=SourceDocument,
    response_description="A single sample record.",
    responses={
        404: {
            "model": ErrorDetailResponse,
            "description": "No sample was found for the supplied sample name or ID.",
            "content": {
                "application/json": {"example": {"detail": "Sample not found"}}
            },
        },
        502: {
            "model": ErrorDetailResponse,
            "description": (
                "The sample could not be fetched because the backend could not "
                "query the search service."
            ),
            "content": {
                "application/json": {
                    "example": {"detail": "Elasticsearch error: connection failed"}
                }
            },
        },
    },
)
def get_sample(
    name: str = Path(
        ...,
        description="Sample name or identifier (for example HG00096).",
        example="HG00096",
    ),
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


@router.post(
    "/_search/{filename}.tsv",
    include_in_schema=False,
    responses={
        200: {
            "content": {"text/tab-separated-values": {}},
            "description": "TSV download of sample search results",
        }
    },
)
async def export_samples_tsv(
    filename: str,
    request: Request,
    json: Optional[str] = Form(
        None,
        description=(
            "Optional JSON search body (stringified). "
            'Example: {"query": {"match_all": {}}, "size": 100}'
        ),
        example='{"query": {"match_all": {}}, "size": 100}',
    ),
) -> Response:
    return await export_tsv_response(
        request=request,
        json_form=json,
        index=INDEX,
        filename=filename,
        size_cap=settings.ES_EXPORT_SIZE_CAP,
        default_fields=["_id", "name", "sex"],
        rewrite=rewrite_terms_for_samples,
    )
