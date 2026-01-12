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
from app.api.schemas import SearchResponse

router = APIRouter(prefix="/beta/file", tags=["file"])
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
    summary="Search files",
    response_model=SearchResponse,
    response_description="Normalised Elasticsearch response for file search",
)
def beta_search_files(
    body: Optional[Dict[str, Any]] = Body(
        None,
        example={
            "query": {"match_all": {}},
            "size": 25,
            "sort": [{"dataType.keyword": "asc"}],
            "_source": ["url", "md5", "dataType"],
        },
        description=(
            "Elasticsearch search payload; size:-1 is capped server-side. "
            "Defaults will add minimal _source fields if omitted."
        ),
    )
) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(
            gate_short_text(2), rewrite_terms_for_file, rewrite_match_queries
        ),
        ensure=_ensure_file_query,
    )


@router.post(
    "/_search/{filename}.tsv",
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
