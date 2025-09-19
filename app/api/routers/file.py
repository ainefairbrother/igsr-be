"""
File router
=================
FE path: /api/beta/file/*  ->  here: /beta/file/*
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

router = APIRouter(prefix="/beta/file", tags=["file"])
INDEX = settings.INDEX_FILE

# -------------------------- Helpers ---------------------------------

def _ensure_file_query(body: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    body = body or {}
    if "_source" not in body:
        body["_source"] = ["url", "md5", "dataType", "analysisGroup", "dataCollections", "samples"]
    return body

# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def beta_search_files(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=compose_rewrites(gate_short_text(2), rewrite_terms_for_file, rewrite_match_queries),
        ensure=_ensure_file_query
    )


@router.post("/_search/{filename}.tsv")
async def export_files_tsv(
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
        default_fields=[
            "url",
            "md5",
            "dataType",
            "analysisGroup",
            "dataCollections",
            "samples",
            "populations"
            ],
        rewrite=compose_rewrites(rewrite_terms_for_file, rewrite_match_queries),
    )