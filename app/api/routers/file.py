# app/api/routers/file.py
"""
File router
=================
FE path: /api/beta/file/*  ->  here: /beta/file/*
"""

from typing import Any, Dict, Optional
from fastapi import APIRouter, Body, Response, Form, Request

from app.core.config import settings
from app.lib.search_utils import run_search
from app.lib.dl_utils import export_tsv_response
from app.lib.es_utils import rewrite_terms_for_file

router = APIRouter(prefix="/beta/file", tags=["file"])
INDEX = settings.INDEX_FILE

# ------------------------------ Endpoints ------------------------------------

@router.post("/_search")
def search_files(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/file/_search
    """
    return run_search(
        INDEX,
        body,
        size_cap=settings.ES_ALL_SIZE_CAP,
        rewrite=rewrite_terms_for_file
    )

@router.post("/_search/{filename}.tsv")
async def export_files_tsv(
    filename: str,
    request: Request,
    json_form: Optional[str] = Form(None, alias="json"),
) -> Response:
    DEFAULT_FIELDS = ["url", "md5", "dataType", "analysisGroup",
                      "dataCollections", "samples", "populations"]

    return await export_tsv_response(
        request=request,
        json_form=json_form,
        index=INDEX,
        filename=filename,
        size_cap=settings.ES_ALL_SIZE_CAP,
        default_fields=DEFAULT_FIELDS,
        rewrite=rewrite_terms_for_file,
    )