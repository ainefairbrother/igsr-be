# app/api/routers/sitemap.py
from typing import Any, Dict, Optional, List, Callable
from fastapi import APIRouter, Body, HTTPException, Query

from app.core.config import settings
from app.services.es import es
from app.lib.es_utils import normalise_es_response

router = APIRouter(prefix="/sitemap", tags=["sitemap"])

SAMPLE_INDEX = settings.INDEX_SAMPLE
POP_INDEX    = settings.INDEX_POPULATION
DC_INDEX     = settings.INDEX_DATA_COLLECTIONS
# Intentionally no FILE_INDEX here – files list & total come from /api/beta/file/_search

# ------------------------------- Helpers ------------------------------------ #

def _extract_q_from_body(body: Optional[Dict[str, Any]]) -> str:
    """
    Pull a free-text query from typical ES request bodies:
    multi_match, simple_query_string, query_string, match_phrase, match.
    """
    if not isinstance(body, dict):
        return ""
    qnode = body.get("query") or body

    def _take_query(node: Dict[str, Any], key: str) -> Optional[str]:
        val = node.get(key)
        if isinstance(val, dict):
            q = val.get("query")
            if q:
                return str(q).strip()
        return None

    if isinstance(qnode, dict):
        for key in ("multi_match", "simple_query_string", "query_string"):
            q = _take_query(qnode, key)
            if q:
                return q

        mp = qnode.get("match_phrase")
        if isinstance(mp, dict):
            for _, spec in mp.items():
                if isinstance(spec, dict) and spec.get("query"):
                    return str(spec["query"]).strip()
                if isinstance(spec, str):
                    return spec.strip()

        m = qnode.get("match")
        if isinstance(m, dict):
            for _, spec in m.items():
                if isinstance(spec, dict) and spec.get("query"):
                    return str(spec["query"]).strip()
                if isinstance(spec, str):
                    return spec.strip()

    return ""

def _search(index: str, body: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = es.search(index=index, body=body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e
    return normalise_es_response(resp)

def _empty_resp() -> Dict[str, Any]:
    return {
        "took": 0,
        "timed_out": False,
        "hits": {"total": 0, "max_score": 0.0, "hits": []},
        "aggregations": {},
    }

def _term_or_wildcard(
    field: str,
    q: str,
    *,
    allow_wildcard: bool = True,
    min_len: int = 2,
    case_insensitive: bool = True,
) -> List[Dict[str, Any]]:
    """
    Exact term plus (optionally) a contains-style wildcard on a keyword field.
    """
    clauses: List[Dict[str, Any]] = [{"term": {field: q}}]
    if allow_wildcard and len(q) >= min_len:
        wc: Dict[str, Any] = {"value": f"*{q}*"}
        if case_insensitive:
            wc["case_insensitive"] = True
        clauses.append({"wildcard": {field: wc}})
    return clauses

def _box_body(
    q: str,
    *,
    fields: List[str],
    size: int,
    sort: List[Dict[str, Any]],
    extra_should: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    """
    Build a standard 'box' query: OR of term+wildcard on each field, optional extras, sorted.
    """
    should: List[Dict[str, Any]] = []
    for f in fields:
        should += _term_or_wildcard(f, q)
    if extra_should:
        should += extra_should(q)

    return {
        "size": size,
        "track_total_hits": True,
        "query": {"bool": {"should": should, "minimum_should_match": 1}},
        "sort": sort,
        "_source": True,
    }

BOXES = {
    "samples": {
        "index": SAMPLE_INDEX,
        "fields": ["name.keyword"],
        "sort": [{"name.keyword": "asc"}],
        "extra_should": lambda q: [{"term": {"_id": q}}],
    },
    "populations": {
        "index": POP_INDEX,
        "fields": ["elasticId.keyword", "code.keyword", "name.keyword"],
        "sort": [
            {"superpopulation.display_order": {"order": "asc"}},
            {"name.keyword": {"order": "asc"}},
        ],
        "extra_should": None,
    },
    "dataCollections": {
        "index": DC_INDEX,
        "fields": ["title.keyword"],
        "sort": [{"title.keyword": "asc"}],
        "extra_should": None,
    },
}

# -------------------------------- Endpoint ---------------------------------- #

@router.api_route("/_search", methods=["GET", "POST"])
def search_sitemap(
    body: Optional[Dict[str, Any]] = Body(None),
    q: Optional[str] = Query(
        None, description="Free-text query; if omitted, inferred from an ES-style body."
    ),
    size: Optional[int] = Query(
        None, ge=1, description="Max results per box (defaults to 10, capped by ES_ALL_SIZE_CAP)."
    ),
) -> Dict[str, Any]:
    """
    Returns the three boxes for the search page:
      - samples
      - populations
      - dataCollections

    NOTE: The “matching data files” panel is intentionally omitted here;
    the FE loads it from /api/beta/file/_search.
    """
    # 1) Decide query text
    raw_q = (q or "").strip() or _extract_q_from_body(body)

    # 2) Decide size (query param wins, then body.size, else 10), clamp to cap
    requested_size = size
    if requested_size is None:
        bsize = (body or {}).get("size")
        requested_size = bsize if isinstance(bsize, int) and bsize > 0 else 10
    capped_size = min(int(requested_size), int(settings.ES_ALL_SIZE_CAP))

    # 3) No query → empty boxes (plus an empty 'files' stub for FE convenience)
    if not raw_q:
        empty = _empty_resp()
        return {
            "samples": empty,
            "populations": empty,
            "dataCollections": empty,
            "files": empty,
        }

    # 4) Build & run the three box searches from the spec
    results: Dict[str, Dict[str, Any]] = {}
    for name, spec in BOXES.items():
        body_spec = _box_body(
            raw_q,
            fields=spec["fields"],
            size=capped_size,
            sort=spec["sort"],
            extra_should=spec.get("extra_should"),
        )
        results[name] = _search(spec["index"], body_spec)

    # 5) Return boxes + empty files stub
    results["files"] = _empty_resp()
    return results