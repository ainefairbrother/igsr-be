from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from fastapi import HTTPException, Request, Response
import json
from app.services.es import es

modified_json = Union[str, int, float, bool, None, List[Any], Dict[str, Any]]
def get_nested(source: Dict[str, Any], path: str) -> modified_json:
    """
    Resolve a dotted _source path for TSV export, for example a.b.c.

    If a segment points to a list of objects, collect that field from each item
    and drop empty values.
    """
    parts = path.split(".")
    current: Any = source
    for p in parts:
        if isinstance(current, list):
            collected: List[Any] = []
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


def to_tsv_cell(value: Any, sep: str = ",") -> str:
    """
    Turn a Python value into a single TSV cell.

    Lists are joined by sep with empty items skipped. Dicts are rendered as
    compact JSON. Scalars are converted to strings. Tabs and newlines are removed.
    None becomes an empty string.
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


def iter_hits_as_rows(hits: Iterable[Dict[str, Any]], columns: List[str], sep: str = ",") -> Iterable[str]:
    """
    Yield tab-separated lines for TSV downloads from Elasticsearch hits.

    For each hit, build a row for the requested columns. _id and _index come
    from the hit itself; all other columns are read from _source using
    get_nested, then formatted with to_tsv_cell.
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
                val = get_nested(src, col)
            row.append(to_tsv_cell(val, sep=sep))
        yield "\t".join(row)
        

RewriteFn = Callable[[Any], Any]
async def export_tsv_response(
    *,
    request: Request,
    json_form: Optional[str],
    index: str,
    filename: str,
    size_cap: int,
    default_fields: List[str],
    rewrite: Optional[RewriteFn] = None,
) -> Response:
    """
    Build a TSV download response from an ES index.

    - Accepts payload via JSON body or multipart form field 'json'
    - Optionally rewrites term/terms filters (e.g. .title -> .title.keyword)
    - Caps size to protect the cluster
    - Queries ES and renders hits to a TSV using iter_hits_as_rows
    """
    # Parse payload
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
                raise HTTPException(status_code=422, detail="Missing JSON body or form field 'json'")
            try:
                payload = json.loads(raw)
            except Exception as e:
                raise HTTPException(status_code=422, detail=f"Invalid form 'json': {e}")

    # Extract request parts
    fields: List[str] = list(payload.get("fields") or [])
    column_names: List[str] = list(payload.get("column_names") or fields)
    query: Dict[str, Any] = payload.get("query") or {"match_all": {}}

    # Field rewrites for exact matching
    if rewrite:
        query = rewrite(query)

    # Cap size
    size = payload.get("size")
    if not isinstance(size, int) or size < 0 or size > size_cap:
        size = size_cap

    # Query ES
    try:
        resp = es.search(
            index=index,
            body={"query": query, "_source": True, "size": size, "track_total_hits": True},
            ignore_unavailable=True,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}")

    hits = (resp.get("hits") or {}).get("hits") or []

    # Columns and header
    if not fields:
        fields = default_fields
    header = "\t".join(column_names) if column_names and len(column_names) == len(fields) else "\t".join(fields)

    # Build TSV
    lines = [header] if header else []
    lines.extend(iter_hits_as_rows(hits, fields))
    tsv = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")

    # Response
    return Response(
        content=tsv,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}.tsv"',
            "Cache-Control": "no-store",
        },
    )