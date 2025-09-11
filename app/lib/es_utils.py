from typing import Any, Dict

def normalise_es_response(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Shape an Elasticsearch response into what the front end expects.

    - Convert hits.total from an object to a plain int.
    - Ensure hits.max_score is always a number (use 0.0 when missing).
    - Always include an aggregations object (empty when not present).

    Returns a dict with took, timed_out, hits and aggregations.
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
    Walk an Elasticsearch query and rewrite only term or terms fields using field_map,
    so exact matches hit the right subfield (for example a .keyword subfield).

    Leaves fields already pointing at .keyword and does not
    mutate the input - returns a new structure.
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
    Apply the field rewrites the Samples front end expects for exact filters.

    Ensures filters target exact-match fields on the sample index:
    - dataCollections.title and dataCollections.title.std -> dataCollections.title.keyword
    - populations.* fields (elasticId, code, name, superpopulationCode, superpopulationName)
      -> their .keyword subfields

    These populations.* mappings are specific to the sample index and are not
    present on the population index.
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
    Apply the field rewrites the Population front end expects for exact filters.

    Populations are filtered by data collection title only, so we map:
    - dataCollections.title and dataCollections.title.std -> dataCollections.title.keyword
    """
    field_map = {
        "dataCollections.title": "dataCollections.title.keyword",
        "dataCollections.title.std": "dataCollections.title.keyword",
    }
    return rewrite_terms_to_keyword(node, field_map)

def rewrite_terms_for_file(node: Any) -> Any:
    field_map = {
        # FE sometimes sends these from shared UI
        "dataCollections.title": "dataCollections.keyword",
        "dataCollections.title.std": "dataCollections.keyword",
        # Native file fields
        "dataCollections": "dataCollections.keyword",
        "analysisGroup": "analysisGroup.keyword",
        "dataType": "dataType.keyword",
        "samples": "samples.keyword",
        "populations": "populations.keyword",
        "url": "url.keyword",
    }
    return rewrite_terms_to_keyword(node, field_map)