# app/api/routers/samples.py
"""
Samples router
==============
FE path: /api/beta/sample/*  →  here: /beta/sample/*

Description
-----------
Queries Elasticsearch for the Samples table and TSV export with a small set of
compatibility adjustments so the current FE keeps working against the current index

- `size:-1` is capped to `settings.ES_ALL_SIZE_CAP`
- if no sort is provided we default to `name.keyword` ascending
- `track_total_hits=True` so totals are exact
- the FE sometimes filters on `dataCollections.title` or `dataCollections.title.std`
  inside `term/terms` queries, so rewrite those field names to
  `dataCollections.title.keyword` for exact matches
- TSV export accepts a list of `_source` paths and streams a tab-separated file,
  joining arrays with commas

Nginx strips the `/api` prefix so FastAPI sees requests under `/beta/sample/*`
All responses are normalised via `normalise_es_response` to match the legacy FE shape
"""

from fastapi import APIRouter, HTTPException, Response, Form, Body, Path, Request
from typing import Any, Dict, Iterable, List, Optional, Union
import json

from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

router = APIRouter(prefix="/beta/sample", tags=["samples"])

INDEX = settings.INDEX_SAMPLE


# ------------------------------- Helpers ------------------------------------ #

def _get_nested(source: Dict[str, Any], path: str) -> Union[str, int, float, bool, None, List[Any], Dict[str, Any]]:
    """
    Fetch a dotted path (e.g. 'populations.code') from _source

    - On lists of dicts collect the child values across the list
    - Drop None/empty values when collecting to avoid trailing separators
    """
    parts = path.split(".")
    current: Any = source
    for p in parts:
        if isinstance(current, list):
            collected: List[str] = []
            for item in current:
                if isinstance(item, dict):
                    v = item.get(p)
                    if isinstance(v, list):
                        collected.extend([vv for vv in v if vv not in (None, "", [])])
                    elif v not in (None, "", []):
                        collected.append(v)
            current = collected
        elif isinstance(current, dict):
            current = current.get(p)
        else:
            return None
    return current


def _to_tsv_cell(value: Any, sep: str = ",") -> str:
    """
    Convert nested or array values to a TSV-friendly string

    - list -> joined by `sep` with no extra spaces, dropping empty entries
    - dict -> compact JSON
    - scalars -> str
    - always strip tabs and newlines
    """
    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        s = str(value)
    elif isinstance(value, list):
        flat: List[str] = []
        for v in value:
            if v in (None, "", []):
                continue
            if isinstance(v, (int, float, bool)):
                flat.append(str(v))
            elif isinstance(v, dict):
                flat.append(json.dumps(v, separators=(",", ":"), ensure_ascii=False))
            else:
                flat.append(str(v))
        s = sep.join(flat)
    elif isinstance(value, dict):
        s = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    else:
        s = str(value)
    return s.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _iter_hits_as_rows(hits: Iterable[Dict[str, Any]], columns: List[str], sep: str = ",") -> Iterable[str]:
    """
    Yield TSV lines for the requested columns

    - '_id' / '_index' come from the hit itself
    - all other paths are read from _source
    """
    for h in hits:
        src = h.get("_source", {}) or {}
        row: List[str] = []
        for col in columns:
            if col == "_id":
                val = h.get("_id")
            elif col == "_index":
                val = h.get("_index")
            else:
                val = _get_nested(src, col)
            row.append(_to_tsv_cell(val, sep=sep))
        yield "\t".join(row)


# ---- FE 'Filter by data collection' (title.std → title.keyword) --------------

def _rewrite_dc_title_to_keyword(node: Any) -> Any:
    """
    Map FE references to dataCollections.title(.std) → dataCollections.title.keyword
    inside term/terms bodies, other nodes pass through untouched
    """
    def _fix_field(field: str) -> str:
        if field in ("dataCollections.title", "dataCollections.title.std"):
            return "dataCollections.title.keyword"
        return field

    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            if k in ("term", "terms") and isinstance(v, dict):
                out[k] = { _fix_field(f): _rewrite_dc_title_to_keyword(fv) for f, fv in v.items() }
            else:
                out[k] = _rewrite_dc_title_to_keyword(v)
        return out
    if isinstance(node, list):
        return [_rewrite_dc_title_to_keyword(x) for x in node]
    return node


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

    # exact totals and stable default sort
    es_body.setdefault("track_total_hits", True)
    if "sort" not in es_body:
        es_body["sort"] = [{"name.keyword": {"order": "asc"}}]

    # FE 'data collection' filter: title.std → title.keyword
    es_body = _rewrite_dc_title_to_keyword(es_body)

    # query ES
    try:
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    return normalise_es_response(resp)


# for dev
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

    Payload:
      - fields: list of dotted _source paths plus special "_id" / "_index"
      - column_names: optional header labels, same length as fields
      - query: ES query, defaults to match_all
      - size: integer, capped server-side

    Response:
      - content type text/tab-separated-values
      - arrays joined by commas, tabs and newlines stripped
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
    lines.extend(_iter_hits_as_rows(hits, fields))
    tsv = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")

    return Response(
        content=tsv,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}.tsv"',
            "Cache-Control": "no-store",
        },
    )