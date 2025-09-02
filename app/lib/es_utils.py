from typing import Any, Dict, Mapping

def normalise_es_response(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalise the ES response so that it is as expected by the FE.

    Key adjustments:
    - ES returns hits.total as an object: {"value": N, "relation": "eq|gte"},
      but the FE expects a plain int, so coerce to plain int.
    - Some queries to ES return max_score = null; FE code can assume it's a number.
      We set it to 0.0 if ES returned null/None.
    - Always return "aggregations" field (empty object if none), so FE templates
      don't need to null-check before accessing it.
    """
    # Copy standard ES fields, defaulting to safe values
    took = resp.get("took", 0)
    timed_out = resp.get("timed_out", False)

    # Normalise the "hits" block
    hits = resp.get("hits", {}) or {}
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    hits["total"] = total

    # Ensure max_score is always a number (FE sometimes treats it as numeric)
    if hits.get("max_score") is None:
        hits["max_score"] = 0.0

    # Always include "aggregations" even if empty
    aggs = resp.get("aggregations", {}) or {}

    # Return the shape that the FE expects
    return {"took": took, "timed_out": timed_out, "hits": hits, "aggregations": aggs}