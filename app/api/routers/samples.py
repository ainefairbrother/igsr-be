# app/api/routers/samples.py
"""
Samples router
==============
FE path: /api/beta/sample/*  →  here: /beta/sample/*
"""

from fastapi import APIRouter, HTTPException, Response, Form, Body, Path, Request
from typing import Any, Dict, Iterable, List, Optional, Union
import json

from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response
from app.lib.es_utils import rewrite_terms_for_samples
from app.lib.dl_utils import iter_hits_as_rows

router = APIRouter(prefix="/beta/sample", tags=["samples"])
INDEX = settings.INDEX_SAMPLE


# ------------------------------ Endpoints ------------------------------------


@router.post("/_search")
def search_samples(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/sample/_search

    Compatibility tweaks:
      - size:-1 → capped to ES_ALL_SIZE_CAP
      - track_total_hits=True for exact totals
      - default sort by name.keyword asc if none provided
      - rewrite dataCollections.title(.std) → .keyword in term/terms
    """
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # handle “return all”
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # exact totals
    es_body.setdefault("track_total_hits", True)

    # FE 'data collection' filter: title.std to title.keyword
    es_body = rewrite_terms_for_samples(es_body)

    # query ES
    try:
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    return normalise_es_response(resp)


# for dev
# curl -s -XGET http://localhost:8080/api/beta/sample/_search | jq
@router.get("/_search")
def search_samples_get() -> Dict[str, Any]:
    return search_samples({"query": {"match_all": {}}, "size": 25})


@router.get("/{name}")
def get_sample(name: str = Path(..., description="Sample identifier (often the ES _id)")) -> Dict[str, Any]:
    """
    GET /beta/sample/{name}

    Response shape matches FE expectations: { "_source": { ...Sample... } }
    """
    # 1) try by ES _id first for speed
    try:
        doc = es.get(index=INDEX, id=name, ignore=[404])
        if doc and doc.get("found"):
            return {"_source": doc.get("_source", {})}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # 2) fallback: search by unique name.keyword
    try:
        resp = es.search(
            index=INDEX,
            body={"size": 1, "query": {"term": {"name.keyword": name}}, "_source": True},
            ignore_unavailable=True,
        )
        hit = (resp.get("hits", {}) or {}).get("hits", [])
        if hit:
            return {"_source": hit[0].get("_source", {})}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    raise HTTPException(status_code=404, detail="Sample not found")


@router.post("/_search/{filename}.tsv")
async def export_samples_tsv(
    filename: str,
    request: Request,
    json_form: Optional[str] = Form(None, alias="json"),
) -> Response:
    """
    POST /beta/sample/_search/{filename}.tsv

    Payload
    -------
    - fields: list of dotted _source paths plus special "_id" / "_index"
    - column_names: optional header labels, same length as fields
    - query: ES query, defaults to match_all
    - size: integer, capped server-side

    Behaviour
    ---------
    - rewrites any dataCollections.title(.std) terms to dataCollections.title.keyword
      so data-collection filters work on exact values
    - returns a TSV where arrays are joined by commas and tabs/newlines are stripped
    """
    # parse payload from form field, raw JSON body, or manual form parse
    payload: Dict[str, Any] = {}
    if json_form is not None:
        try:
            payload = json.loads(json_form)
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Invalid form 'json': {e}")
    else:
        ctype = (request.headers.get("content-type") or "").lower()
        if "application/json" in ctype:
            try:
                payload = await request.json()
            except Exception as e:
                raise HTTPException(status_code=422, detail=f"Invalid JSON body: {e}")
        else:
            form = await request.form()
            raw = form.get("json")
            if not raw:
                raise HTTPException(status_code=422, detail="Missing form field 'json'")
            try:
                payload = json.loads(raw)
            except Exception as e:
                raise HTTPException(status_code=422, detail=f"Invalid form 'json': {e}")

    # extract requested columns and query
    fields: List[str] = list(payload.get("fields") or [])
    column_names: List[str] = list(payload.get("column_names") or fields)
    query: Dict[str, Any] = payload.get("query") or {"match_all": {}}

    # make DC filters exact-match by using the keyword subfield
    query = rewrite_terms_for_samples(query)

    # cap export size
    size = payload.get("size")
    if not isinstance(size, int) or size < 0 or size > settings.ES_ALL_SIZE_CAP:
        size = settings.ES_ALL_SIZE_CAP

    # query ES for export
    try:
        resp = es.search(
            index=INDEX,
            body={"query": query, "_source": True, "size": size, "track_total_hits": True},
            ignore_unavailable=True,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}")

    hits = (resp.get("hits") or {}).get("hits") or []

    # default fields if none provided
    if not fields:
        fields = ["_id", "name", "sex"]

    header = "\t".join(column_names) if column_names and len(column_names) == len(fields) else "\t".join(fields)

    # build TSV
    lines = [header] if header else []
    lines.extend(iter_hits_as_rows(hits, fields))
    tsv = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")

    return Response(
        content=tsv,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}.tsv"',
            "Cache-Control": "no-store",
        },
    )