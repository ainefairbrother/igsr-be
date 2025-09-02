# app/api/routers/samples.py
from fastapi import APIRouter, HTTPException, Response, Form, Body, Path, Request
from typing import Any, Dict, Iterable, List, Optional, Union
import json
from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

router = APIRouter(prefix="/beta/sample", tags=["samples"])

INDEX = settings.INDEX_SAMPLE


# ------------------------ Helpers & normalisers -------------------------------

def _get_nested(source: Dict[str, Any], path: str) -> Union[str, int, float, bool, None, List[Any], Dict[str, Any]]:
    """
    Helper for formatting for TSV export.
    
    Fetch nested fields from _source given dotted paths like 'populations.code':
    - For lists of dicts, collect the child values across the list
    - Drops None/empty values when collecting lists to prevent trailing separators
    """
    parts = path.split(".")
    current: Any = source
    for p in parts:
        if isinstance(current, list):
            # apply the remaining key to each element and drop Nones/empties
            collected: List[Any] = []
            for item in current:
                if isinstance(item, dict):
                    v = item.get(p)
                    # flatten a level if v is list; otherwise keep as scalar
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
    Helper for formatting for TSV export.
    
    Convert nested/array values to a TSV-friendly string:
      - list -> joined by `sep` with NO extra spaces, dropping empty entries
      - dict -> JSON
      - scalar -> string
      - always strip tabs/newlines
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
    Helper for formatting for TSV export.
    
    Yield TSV lines for given column paths:
    - '_id' / '_index' come from the hit itself (hit metadata)
    - Everything else comes from inside _source
    - Lists are joined with `sep` (default ',') and empties are dropped by _to_tsv_cell
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


def _rewrite_dc_title_to_keyword(node: Any) -> Any:
    """
    Compatibility helper for FE 'Filter by data collection' filter button
    Field renamed to dataCollections.title.keyword wherever it appears in the body
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
    
    FE call to Elasticsearch for sample, with some small
    adjustments to the request and response to match FE expectations:
    - The FE uses size:-1, meaning "return all". ES rejects negative sizes, 
      so any negative size is mapped to a large, safe default (10,000, set in core/config.py)
    - Provide a sensible default sort if none supplied
      
    Other than these things, the request body is kept as-is and a normalised ES response 
    is returned, preserving the fields the FE uses
    """
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # Handle "return all" from the FE (size:-1)
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # Compute accurate totals
    es_body.setdefault("track_total_hits", True)

    # Provide a stable default sort so the table doesn't change between requests
    if "sort" not in es_body:
        es_body["sort"] = [{"name.keyword": {"order": "asc"}}]

    # Map FE term/terms on dataCollections.title(.std) to .keyword so ES can match
    es_body = _rewrite_dc_title_to_keyword(es_body)

    try:
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    return normalise_es_response(resp)


# this is for dev
# test with: curl -s -XGET http://localhost:8080/api/beta/sample/_search | jq
@router.get("/_search")
def search_samples_get() -> Dict[str, Any]:
    return search_samples({"query": {"match_all": {}}, "size": 25})


@router.get("/{name}")
def get_sample(name: str = Path(..., description="Sample identifier (often the ES _id)")) -> Dict[str, Any]:
    """
    GET /beta/sample/{name}
    
    Return a single sample document in the shape that the FE expects:
      { "_source": { ...Sample... } }

    First try ES GET by id (fastest). If that fails (404), fall back to searching
    by a unique field (name.keyword == {name})
    """
    # 1. Quickest: GET by document id
    try:
        doc = es.get(index=INDEX, id=name, ignore=[404])
        if doc and doc.get("found"):
            return {"_source": doc.get("_source", {})}
    except Exception as e:
        # Non-404 ES errors → 502
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # 2. Slower, fallback: search by name.keyword
    # only runs if the _id is not the sample name
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
            return {"_source": hit[0].get("_source", {})}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # Not found by id or by name
    raise HTTPException(status_code=404, detail="Sample not found")


@router.post("/_search/{filename}.tsv")
async def export_samples_tsv(
    filename: str,
    request: Request,
    json_form: Optional[str] = Form(None, alias="json"),
) -> Response:
    """
    POST /beta/sample/_search/{filename}.tsv
    
    Multiple input paths are used because:
    - The old FE submits a plain <form> with one field named "json"
      (might arrive in urlencoded or multipart format).
    - Dev/tools sometimes send a raw application/json body.
    We therefore accept: (1) Form-bound `json_form`, (2) raw JSON body,
    or (3) manual form parse fallback, which avoids errors when headers 
    are unexpected and simplifies curl requests.

    Expected payload keys:
      - fields: list of dotted _source paths (plus special "_id" / "_index")
      - column_names: optional header labels (same length as fields)
      - query: ES query (defaults to match_all)
      - size: integer (capped server-side)

    Response:
      - text/tab-separated-values
      - arrays joined by commas; tabs/newlines stripped
    """
    # Parse payload from (1) form field, or (2) raw JSON body, or (3) explicit form parse
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

    # Extract requested columns & query
    fields: List[str] = list(payload.get("fields") or [])
    column_names: List[str] = list(payload.get("column_names") or fields)
    query: Dict[str, Any] = payload.get("query") or {"match_all": {}}

    # Set export size to configured limit if not provided or out of range
    size = payload.get("size")
    if not isinstance(size, int) or size < 0 or size > settings.ES_ALL_SIZE_CAP:
        size = settings.ES_ALL_SIZE_CAP

    # Fetch from ES (_source is needed for TSV extraction, which contains the original JSON document)
    try:
        resp = es.search(
            index=INDEX,
            body={"query": query, "_source": True, "size": size, "track_total_hits": True},
            ignore_unavailable=True,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}")

    hits = (resp.get("hits") or {}).get("hits") or []

    # If FE didn’t provide fields, fall back to a minimal useful set
    if not fields:
        fields = ["_id", "name", "sex"]

    # Use column_names if provided, otherwise echo 'fields'
    header = "\t".join(column_names) if column_names and len(column_names) == len(fields) else "\t".join(fields)

    # Build TSV in-memory
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