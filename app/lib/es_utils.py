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
  

def rewrite_terms_to_keyword(node: Any, field_map: Dict[str, str]) -> Any:
    """
    Walk an ES query body and rewrite only `term`/`terms` field names so exact matches
    hit `.keyword` (or another target) as per `field_map`
    
    - Idempotent: leaves fields already ending in `.keyword` alone
    - Pure: returns a new structure without mutating the input
    """
    def _fix(field: str) -> str:
        if field.endswith(".keyword"):
            return field
        return field_map.get(field, field)

    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k in ("term", "terms") and isinstance(v, dict):
                out[k] = { _fix(f): rewrite_terms_to_keyword(vv, field_map) for f, vv in v.items() }
            else:
                out[k] = rewrite_terms_to_keyword(v, field_map)
        return out
    if isinstance(node, list):
        return [rewrite_terms_to_keyword(x, field_map) for x in node]
    return node


def rewrite_terms_for_samples(node: Any) -> Any:
    """
    Samples-only wrapper around rewrite_terms_to_keyword
    The Samples FE sometimes sends `term/terms` filters against analysed text
    fields; exact matching in ES requires querying the `.keyword` subfields.
    For samples this covers:
      - dataCollections.title(.std) → dataCollections.title.keyword
      - populations.* (elasticId, code, name, superpopulation*) → *.keyword
      - populations.* exists on the sample index but not on the population index
    """
    field_map = {
        "dataCollections.title": "dataCollections.title.keyword",
        "dataCollections.title.std": "dataCollections.title.keyword",
        # population filters used by the FE
        "populations.elasticId": "populations.elasticId.keyword",
        "populations.code": "populations.code.keyword",
        "populations.name": "populations.name.keyword",
        "populations.superpopulationCode": "populations.superpopulationCode.keyword",
        "populations.superpopulationName": "populations.superpopulationName.keyword",
    }
    return rewrite_terms_to_keyword(node, field_map)


def rewrite_terms_for_population(node: Any) -> Any:
    """
    Population-only wrapper around rewrite_terms_to_keyword
    The Population FE may filter by data-collection title; exact matching
    requires the `.keyword` subfield:
      - dataCollections.title(.std) → dataCollections.title.keyword
    """
    field_map = {
        "dataCollections.title": "dataCollections.title.keyword",
        "dataCollections.title.std": "dataCollections.title.keyword",
    }
    return rewrite_terms_to_keyword(node, field_map)