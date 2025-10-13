from typing import Any, Dict, Callable, Optional, List
from fastapi import HTTPException
from app.services.es import es
from app.lib.es_utils import normalise_es_response

RewriteFn   = Callable[[Any], Any]
EnsureFn    = Callable[[Dict[str, Any]], Dict[str, Any]]
PostprocFn  = Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]

def run_search(
    index: str,
    body: Optional[Dict[str, Any]],
    *,
    size_cap: int,
    rewrite: Optional[RewriteFn] = None,
    default_sort: Optional[List[Dict[str, Any]]] = None,
    ensure: Optional[EnsureFn] = None,
    postprocess: Optional[PostprocFn] = None
) -> Dict[str, Any]:
    """
    Run a standardised Elasticsearch search and return a response shaped for the front end.

    Behaviour
    - Uses a match_all query when no body is provided.
    - Treats size:-1 as "return all" but caps it at size_cap as defined in settings.
    - Ensures track_total_hits is true and applies default_sort when the request has no sort.
    - Router-specific modifications:
      rewrite(body)     modify the request before sending to ES.
      ensure(body)      make final tweaks such as adding minimal _source.
      postprocess(resp, body) adjust the ES response before returning it.
    - Converts ES errors into HTTP 502.
    - Normalises the final payload with normalise_es_response.
    """
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    size = es_body.get("size")
    if size is None:
        es_body["size"] = int(size_cap)
    elif isinstance(size, int):
        if size < 0 or size > size_cap:
            es_body["size"] = size_cap

    # ensure real total hits displayed on the FE, not capped value
    es_body.setdefault("track_total_hits", True)
    
    if default_sort and "sort" not in es_body:
        es_body["sort"] = default_sort
    if rewrite:
        es_body = rewrite(es_body)
    if ensure:
        es_body = ensure(es_body)

    try:
        raw = es.search(index=index, body=es_body, ignore_unavailable=True)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=502, detail="backend_unavailable")

    resp: Dict[str, Any] = getattr(raw, "body", raw)
    if not isinstance(resp, dict):
        resp = dict(resp)
    if postprocess:
        resp = postprocess(resp, es_body)
    return normalise_es_response(resp)