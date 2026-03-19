"""
File router
===========
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from fastapi import APIRouter, Body, Response, Form, Request

from app.core.config import settings
from app.lib.search_utils import run_search
from app.lib.dl_utils import export_tsv_response
from app.lib.es_utils import (
    rewrite_terms_for_file,
    rewrite_match_queries,
    gate_short_text,
    compose_rewrites,
)
from app.api.schemas import SearchResponse, ErrorDetailResponse, SearchRequest

router = APIRouter(prefix="/beta/file", tags=["File"])
INDEX = settings.INDEX_FILE

# -------------------------- Helpers ---------------------------------


def _ensure_file_query(body: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    body = body or {}
    if "_source" not in body:
        body["_source"] = [
            "url",
            "md5",
            "dataType",
            "analysisGroup",
            "dataCollections",
            "samples",
        ]
    return body


# ------------------------------ Endpoints ------------------------------------


@router.post(
    "/_search",
    summary="List all files.",
    description=(
        "Get all files related to IGSR data. Response includes: IDs, URLs, checksums and data types."
    ),
    response_model=SearchResponse,
    response_description="A list of files, plus the total number of matches.",
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
def beta_search_files(
    body: Optional[SearchRequest] = Body(
        None,
        example={
            "query": {"match_all": {}},
            "size": 25,
            "sort": [{"dataType.keyword": "asc"}],
            "_source": ["url", "md5", "dataType"],
        },
        description=(
            "Search filters and options. If size is -1, the API returns as many results as allowed by the server limit. "
            "If _source is not provided, the API returns a default set of key file fields."
        ),
    )
) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body.model_dump(by_alias=True, exclude_none=True) if body else None,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(
            gate_short_text(2), rewrite_terms_for_file, rewrite_match_queries
        ),
        ensure=_ensure_file_query,
    )


@router.post(
    "/_search/{filename}.tsv",
    include_in_schema=False,
    summary="Export file search to TSV",
    response_description="TSV file containing the selected fields",
    responses={
        200: {
            "content": {"text/tab-separated-values": {}},
            "description": "TSV download of file search results",
        }
    },
)
async def export_files_tsv(
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
        default_fields=[
            "url",
            "md5",
            "dataType",
            "analysisGroup",
            "dataCollections",
            "samples",
            "populations",
        ],
        rewrite=compose_rewrites(rewrite_terms_for_file, rewrite_match_queries),
    )
