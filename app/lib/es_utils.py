from typing import Any, Callable, Dict, List, Optional
from functools import reduce
from collections import deque

# ---------------- Normalise ES response -------------------------

def normalise_es_response(resp: Dict[str, Any]) -> Dict[str, Any]:
    took = resp.get("took", 0)
    timed_out = resp.get("timed_out", False)

    hits = resp.get("hits", {}) or {}
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    hits["total"] = total

    if hits.get("max_score") is None:
        hits["max_score"] = 0.0

    aggs = resp.get("aggregations", {}) or {}

    out: Dict[str, Any] = {
        "took": took,
        "timed_out": timed_out,
        "hits": hits,
        "aggregations": aggs,
    }

    if "total" in resp and isinstance(resp["total"], int):
        out["total"] = resp["total"]

    return out

# ---------------- Term/terms to .keyword constants --------------

FIELD_MAP_SAMPLES: Dict[str, str] = {
    "dataCollections.title": "dataCollections.title.keyword",
    "dataCollections.title.std": "dataCollections.title.keyword",
    "populations.elasticId": "populations.elasticId.keyword",
    "populations.code": "populations.code.keyword",
    "populations.name": "populations.name.keyword",
    "populations.superpopulationCode": "populations.superpopulationCode.keyword",
    "populations.superpopulationName": "populations.superpopulationName.keyword",
}

FIELD_MAP_POPULATION: Dict[str, str] = {
    "dataCollections.title": "dataCollections.title.keyword",
    "dataCollections.title.std": "dataCollections.title.keyword",
}

FIELD_MAP_FILE: Dict[str, str] = {
    # FE sometimes uses shared UI names; normalise to the actual exact field.
    "dataCollections.title": "dataCollections.keyword",
    "dataCollections.title.std": "dataCollections.keyword",
    # Native file fields that need exact matches
    "dataCollections": "dataCollections.keyword",
    "analysisGroup": "analysisGroup.keyword",
    "dataType": "dataType.keyword",
    "samples": "samples.keyword",
    "populations": "populations.keyword",
    "url": "url.keyword",
    "url.keywords": "url.keyword",  # legacy plural
}

FIELD_MAP_DATA_COLLECTION: Dict[str, str] = {
    "title": "title.keyword",
    "title.std": "title.keyword",
    "shortTitle": "shortTitle.keyword",
    "shortTitle.std": "shortTitle.keyword",
}

# ---------------- Field normalisation ---------------------------

# Elasticsearch exact-match filters (term/terms) must hit keyword fields
# (untokenised; stored exactly), not analysed text fields (tokenised; e.g.
# "Low coverage WGS" to ["low","coverage","wgs"]). The FE still sends legacy /
# analysed names — e.g. url (to url.keyword), url.keywords (legacy alias,
# normalised to url.keyword), dataCollections.title (to dataCollections.keyword),
# analysisGroup (to analysisGroup.keyword), which which would return 0 hits if sent
# straight to ES. The rewrite step maps these to their exact, modern counterparts
# so existing payloads keep working. Full-text queries (match/multi_match)
# are left unchanged; the rewrite only targets term/terms.

def _normalise_field_to_keyword(field: str, field_map: Optional[Dict[str, str]] = None) -> str:
    """
    Convert a field name to its exact (.keyword) counterpart.

    Priority:
      1) If already *.keyword: keep
      2) If present in field_map: use mapped target
      3) Generic legacy patterns:
         - *.std -> *.keyword
         - *.keywords -> *.keyword
         - url -> url.keyword
         - dataCollections.title -> dataCollections.title.keyword
      4) Otherwise return the field unchanged.
    """
    if not isinstance(field, str):
        return field
    if field.endswith(".keyword"):
        return field
    if field_map and field in field_map:
        return field_map[field]
    if field.endswith(".std"):
        return field[:-4] + ".keyword"
    if field.endswith(".keywords"):
        return field[:-9] + ".keyword"
    if field == "url":
        return "url.keyword"
    if field == "dataCollections.title":
        return "dataCollections.title.keyword"
    return field


def _normalise_fields_list(fields: Any, field_map: Optional[Dict[str, str]] = None) -> List[str]:
    if not isinstance(fields, list):
        return []
    out: List[str] = []
    for f in fields:
        if isinstance(f, str):
            out.append(_normalise_field_to_keyword(f, field_map))
    return out


def _rewrite_terms_to_keyword(node: Any, field_map: Dict[str, str]) -> Any:
    """
    Rewrite field names inside `term` / `terms` clauses to their exact
    (keyword) counterparts using `field_map` + generic rules. Everything else is unchanged.

    Example:
      IN : {"terms": {"dataCollections.title": ["1000 Genomes on GRCh38"]}}
      OUT: {"terms": {"dataCollections.title.keyword": ["1000 Genomes on GRCh38"]}}
    """
    def _fix(field: str) -> str:
        return _normalise_field_to_keyword(field, field_map)

    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            if k in ("term", "terms") and isinstance(v, dict):
                out[k] = { _fix(f): _rewrite_terms_to_keyword(vv, field_map) for f, vv in v.items() }
            else:
                out[k] = _rewrite_terms_to_keyword(v, field_map)
        return out
    if isinstance(node, list):
        return [_rewrite_terms_to_keyword(x, field_map) for x in node]
    return node


# wrappers so that imports to routers can be cleaner
def rewrite_terms_for_samples(node: Any) -> Any:
    return _rewrite_terms_to_keyword(node, FIELD_MAP_SAMPLES)


def rewrite_terms_for_population(node: Any) -> Any:
    return _rewrite_terms_to_keyword(node, FIELD_MAP_POPULATION)


def rewrite_terms_for_file(node: Any) -> Any:
    return _rewrite_terms_to_keyword(node, FIELD_MAP_FILE)


def rewrite_terms_for_data_collection(node: Any) -> Any:
    return _rewrite_terms_to_keyword(node, FIELD_MAP_DATA_COLLECTION)

# ---------------- Rewrite compose helper ------------------------

def compose_rewrites(*fns: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def _chain(node: Any) -> Any:
        return reduce(lambda acc, f: f(acc), fns, node)
    return _chain

# ---------------- Match rewrite helpers -------------------------

def _add_wildcard_if_missing(q: Any) -> str:
    """Add wildcards around a string if none are present."""
    s = str(q or "").strip()
    if "*" in s:
        return s
    return f"*{s}*"


def _normalise_query_text(s: str) -> str:
    """
    Treat '+' as a space when queries arrive URL-encoded (e.g. 'MAGE+RNA-seq').
    We only rewrite when there are no spaces already, to avoid mangling legit
    plus signs in other contexts. This comes as a result of searching in the top 
    right search box on the FE, which URL-encodes the query before sending to the BE.
    """
    s = str(s or "").strip()
    return s.replace("+", " ") if (" " not in s and "+" in s) else s


def rewrite_match_queries(node: Any) -> Any:
    """
    Broad matching:
      - Normalise fields (e.g. *.std / *.keywords / url / dataCollections.title -> *.keyword).
      - Add CI wildcard fallback on every *.keyword field (incl. url.keyword).
      - Wrap in bool.should with minimum_should_match = 1.
    """
    if isinstance(node, dict):
        # multi_match: normalise fields AND query text (decode '+' -> space)
        if "multi_match" in node and isinstance(node["multi_match"], dict):
            mm = dict(node["multi_match"])

            # normalise query text from URL-encoded inputs (top-right search box) (e.g. 'MAGE+RNA-seq')
            q_raw = str(mm.get("query", "")).strip()
            q = _normalise_query_text(q_raw)
            mm["query"] = q

            # normalise fields to their keyword equivalents
            targets = _normalise_fields_list(mm.get("fields", []))
            if targets:
                mm["fields"] = targets

            # keep original multi_match + add wildcard fallbacks
            should: List[Dict[str, Any]] = [{"multi_match": mm}]
            for t in targets:
                if t.endswith(".keyword"):
                    should.append({
                        "wildcard": {t: {"value": _add_wildcard_if_missing(q), "case_insensitive": True}}
                    })

            return {"bool": {"should": should, "minimum_should_match": 1}}

        # Recurse into nested structures
        return {k: rewrite_match_queries(v) for k, v in node.items()}

    if isinstance(node, list):
        return [rewrite_match_queries(x) for x in node]
    return node


def gate_short_text(min_len: int = 2):
    """
    If a free-text query is shorter than `min_len`, replace it with match_none.
    Applies only to texty queries (multi_match, query_string, simple_query_string,
    match, match_phrase). Exact filters (term/terms) are left untouched.
    """
    def _gate(node: Any) -> Any:
        if not isinstance(node, dict):
            return node
        qnode = node.get("query")
        if not isinstance(qnode, dict):
            return node

        def _too_short(val: Any) -> bool:
            s = str(val or "").strip()
            return len(s) < min_len

        # multi_match / query_string / simple_query_string
        for key in ("multi_match", "query_string", "simple_query_string"):
            v = qnode.get(key)
            if isinstance(v, dict) and _too_short(v.get("query")):
                node["query"] = {"match_none": {}}
                return node

        # match / match_phrase can be {"field": {"query": "..."} } or {"field": "..." }
        for key in ("match", "match_phrase"):
            v = qnode.get(key)
            if isinstance(v, dict):
                for spec in v.values():
                    if (isinstance(spec, dict) and _too_short(spec.get("query"))) or \
                       (isinstance(spec, str) and _too_short(spec)):
                        node["query"] = {"match_none": {}}
                        return node

        return node
    return _gate