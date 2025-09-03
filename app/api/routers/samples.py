# app/api/routers/samples.py
"""
Samples router
==============
FE path: /api/beta/sample/*  →  here: /beta/sample/*

Purpose
-------
Minimal pass-through to Elasticsearch with a few compatibility shims so the
legacy FE keeps working against the current sample index:

1) Analysis-group filters/aggregations:
   - FE may send *codes* or short labels (e.g. "low_coverage", "ONT").
   - We normalise those values to the canonical *titles* stored in the sample
     documents (e.g. "Low coverage WGS", "Oxford Nanopore Technologies").
   - Field name remains `dataCollections._analysisGroups` (it is aggregatable
     and has doc values in this index) — we do **not** rewrite to any "flat"
     field.

2) Analysis-group fields in hits:
   - If FE requests `fields: ["dataCollections._analysisGroups"]`, we ensure
     doc values are requested.
   - If ES still doesn't return them (e.g. _source:false), we fetch a minimal
     _source set and synthesise from `dataCollections.sequence/alignment/variants`.
   - Additionally, we **expand** the returned list to include both canonical
     long labels *and* common aliases (code/shortTitle). This makes the FE's
     "blue dot" grid work even when its column labels are short (e.g. "ONT").

Other small tweaks:
- `size:-1` → capped to `settings.ES_ALL_SIZE_CAP`
- `track_total_hits=True` for exact totals
- default sort: `name.keyword` ascending
"""

from fastapi import APIRouter, HTTPException, Response, Form, Body, Path, Request
from typing import Any, Dict, Iterable, List, Optional, Union
import json
import re
import time

from app.services.es import es
from app.core.config import settings
from app.lib.es_utils import normalise_es_response

router = APIRouter(prefix="/beta/sample", tags=["samples"])

INDEX = settings.INDEX_SAMPLE


# ========================= Helpers & normalisers ==============================

def _get_nested(source: Dict[str, Any], path: str) -> Union[str, int, float, bool, None, List[Any], Dict[str, Any]]:
    """
    Fetch a dotted path (e.g. 'populations.code') from _source.

    - On lists of dicts, collect the child values across the list.
    - Drop None/empty values when collecting to avoid trailing separators.
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


def _to_tsv_cell(value: Any, sep: str = ",") -> str:
    """
    Convert nested/array values to a TSV-friendly string:
    - list → joined by `sep` (no extra spaces), dropping empty entries
    - dict → compact JSON
    - scalars → str
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
    Yield TSV lines for the requested columns:
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
    inside term/terms bodies (other nodes pass through untouched).
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


# ---- Technology value normaliser (analysis groups) ---------------------------

def _norm_key(s: str) -> str:
    """Lowercase and strip all non-alphanumerics so we can match many variants."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


# Static fallback synonyms (kept small). Dynamic map (from ES) will override.
_AG_SYNONYMS_STATIC: Dict[str, str] = {
    # exact titles
    _norm_key("Exome"): "Exome",
    _norm_key("Low coverage WGS"): "Low coverage WGS",
    _norm_key("HD genotype chip"): "HD genotype chip",
    _norm_key("Complete Genomics"): "Complete Genomics",
    _norm_key("PCR-free high coverage"): "PCR-free high coverage",
    _norm_key("10X Genomics Chromium"): "10X Genomics Chromium",
    _norm_key("TruSeq Synthetic Long Read"): "TruSeq Synthetic Long Read",
    _norm_key("3.5kb long insert WGS"): "3.5kb long insert WGS",
    _norm_key("7kb mate pair library"): "7kb mate pair library",
    _norm_key("HiC"): "HiC",
    _norm_key("PacBio SMRT genomic"): "PacBio SMRT genomic",
    _norm_key("PacBio SMRT RNA Iso-seq"): "PacBio SMRT RNA Iso-seq",
    _norm_key("Strand specific RNA-seq"): "Strand specific RNA-seq",
    _norm_key("Bionano optical map"): "Bionano optical map",
    _norm_key("Oxford Nanopore Technologies"): "Oxford Nanopore Technologies",
    _norm_key("High coverage WGS"): "High coverage WGS",
    _norm_key("mRNA"): "mRNA",
    _norm_key("High coverage RNA-seq"): "High coverage RNA-seq",
    _norm_key("PacBio CLR"): "PacBio CLR",
    _norm_key("PacBio HiFi"): "PacBio HiFi",
    _norm_key("Illumina NovaSeq 6000"): "Illumina NovaSeq 6000",

    # legacy codes / short labels → canonical titles
    _norm_key("low_coverage"): "Low coverage WGS",
    _norm_key("low cov wgs"): "Low coverage WGS",
    _norm_key("hd_genotype_chip"): "HD genotype chip",
    _norm_key("cg"): "Complete Genomics",
    _norm_key("pcr_free_high"): "PCR-free high coverage",
    _norm_key("pcr-free high cov"): "PCR-free high coverage",
    _norm_key("10x chromium"): "10X Genomics Chromium",
    _norm_key("10x genomics chromium"): "10X Genomics Chromium",
    _norm_key("truseq slr"): "TruSeq Synthetic Long Read",
    _norm_key("3.5kb liwgs"): "3.5kb long insert WGS",
    _norm_key("7kb mate pair"): "7kb mate pair library",
    _norm_key("sv_smrt"): "PacBio SMRT genomic",
    _norm_key("pacbio smrt"): "PacBio SMRT genomic",
    _norm_key("sv_rna_smrt"): "PacBio SMRT RNA Iso-seq",
    _norm_key("pacbio rna"): "PacBio SMRT RNA Iso-seq",
    _norm_key("strand rna-seq"): "Strand specific RNA-seq",
    _norm_key("strand_specific_rna"): "Strand specific RNA-seq",
    _norm_key("bionano"): "Bionano optical map",
    _norm_key("ont"): "Oxford Nanopore Technologies",
    _norm_key("oxford nanopore"): "Oxford Nanopore Technologies",
    _norm_key("high cov wgs"): "High coverage WGS",
    _norm_key("high_coverage"): "High coverage WGS",
    _norm_key("high_cov_rna"): "High coverage RNA-seq",
    _norm_key("hifi"): "PacBio HiFi",
    _norm_key("clr"): "PacBio CLR",
    _norm_key("novaseq"): "Illumina NovaSeq 6000",
    _norm_key("illumina novaseq"): "Illumina NovaSeq 6000",
}

# Small static expansions used only if we cannot read the AG index.
# These are the short labels/codes that appear in the FE column headers.
_AG_STATIC_EXPANSIONS: Dict[str, List[str]] = {
    "Oxford Nanopore Technologies": ["ONT"],
    "Low coverage WGS": ["Low cov WGS", "low_coverage"],
    "PCR-free high coverage": ["PCR-free high cov", "pcr_free_high"],
    "TruSeq Synthetic Long Read": ["TruSeq SLR", "moleculo"],
    "High coverage RNA-seq": ["High cov RNA", "high_cov_RNA"],
    "PacBio HiFi": ["HiFi"],
    "PacBio CLR": ["CLR"],
    "10X Genomics Chromium": ["10X Chromium", "10X_Genomics_Chromium"],
    "3.5kb long insert WGS": ["3.5kb liWGS", "3.5kb_jump"],
    "7kb mate pair library": ["7kb mate pair", "sv_7kb_mate"],
    "PacBio SMRT genomic": ["PacBio SMRT", "sv_smrt"],
    "PacBio SMRT RNA Iso-seq": ["PacBio RNA", "sv_rna_smrt"],
    "Strand specific RNA-seq": ["Strand RNA-seq", "strand_specific_rna"],
    "High coverage WGS": ["High cov WGS", "high_coverage"],
    "Illumina NovaSeq 6000": ["Illumina NovaSeq", "novaseq"],
    "Bionano optical map": ["Bionano", "bionano"],
}

# Dynamic cache (from the analysis-group index)
_AG_DYN_CACHE_TTL_SECS = 300
_AG_DYN_CACHE_TS: float = 0.0
_AG_DYN_SYNONYMS: Dict[str, str] = {}
_AG_DYN_EXPANSIONS: Dict[str, List[str]] = {}


def _visible_ag_title(src: Dict[str, Any]) -> str:
    """Prefer long human titles (description/title), fallback: shortTitle/code."""
    return src.get("description") or src.get("title") or src.get("shortTitle") or src.get("code") or ""


def _load_ag_mappings_from_index() -> None:
    """
    Populate _AG_DYN_SYNONYMS (norm→canonical) and _AG_DYN_EXPANSIONS (canonical→aliases)
    from the analysis-group index. If the index isn't available, leave both empty.
    """
    global _AG_DYN_SYNONYMS, _AG_DYN_EXPANSIONS
    try:
        resp = es.search(
            index=getattr(settings, "INDEX_ANALYSIS_GROUP"),
            body={
                "size": getattr(settings, "ES_ALL_SIZE_CAP", 5000),
                "_source": ["code", "shortTitle", "title", "description", "displayOrder"],
                "sort": [{"displayOrder": {"order": "asc"}}, {"title.keyword": {"order": "asc"}}],
            },
            ignore_unavailable=True,
        )
    except Exception:
        _AG_DYN_SYNONYMS, _AG_DYN_EXPANSIONS = {}, {}
        return

    dyn_syn: Dict[str, str] = {}
    dyn_exp: Dict[str, List[str]] = {}
    for h in (resp.get("hits", {}) or {}).get("hits", []):
        src = h.get("_source", {}) or {}
        canonical = _visible_ag_title(src)
        if not canonical:
            continue
        # Collect all human forms we want to recognise/emit
        aliases: List[str] = []
        for keyish in (src.get("code"), src.get("shortTitle"), src.get("title"), canonical):
            if keyish:
                dyn_syn[_norm_key(keyish)] = canonical
                aliases.append(keyish)
        # Store unique aliases (canonical included)
        if aliases:
            seen, uniq = set(), []
            for a in aliases:
                if a not in seen:
                    seen.add(a)
                    uniq.append(a)
            dyn_exp[canonical] = uniq

    _AG_DYN_SYNONYMS, _AG_DYN_EXPANSIONS = dyn_syn, dyn_exp


def _get_ag_maps():
    """
    Return (synonyms_map, expansions_map).
    - synonyms_map: normalised form → canonical title
    - expansions_map: canonical title → list of alias strings (incl. canonical)
    Dynamic maps (from ES) refresh every _AG_DYN_CACHE_TTL_SECS and override static.
    """
    global _AG_DYN_CACHE_TS
    now = time.time()
    if not _AG_DYN_SYNONYMS or (now - _AG_DYN_CACHE_TS) > _AG_DYN_CACHE_TTL_SECS:
        _load_ag_mappings_from_index()
        _AG_DYN_CACHE_TS = now

    # Dynamic synonyms override static; expansions fall back to a small static set
    synonyms = {**_AG_SYNONYMS_STATIC, **_AG_DYN_SYNONYMS}
    expansions = {**_AG_STATIC_EXPANSIONS, **_AG_DYN_EXPANSIONS}
    # Ensure every canonical we know has at least itself as an alias
    for canon in set(synonyms.values()):
        expansions.setdefault(canon, [canon])
        if canon not in expansions[canon]:
            expansions[canon] = [canon] + [x for x in expansions[canon] if x != canon]
    return synonyms, expansions


def _normalise_ag_value(value: Any) -> Any:
    """Translate FE 'technology' values (codes/short labels) to canonical titles."""
    if not isinstance(value, str):
        return value
    synonyms, _ = _get_ag_maps()
    return synonyms.get(_norm_key(value), value)


def _normalise_ag_values_in_body(node: Any) -> Any:
    """
    Normalise VALUES (NOT field names) for analysis group filters/aggregations:
      - Applies to term/terms on dataCollections._analysisGroups(.keyword)
      - Applies to aggs.terms.include lists on that field
    Keep field name as 'dataCollections._analysisGroups' because that field
    is aggregatable and has doc values in this index.
    """
    target_fields = {"dataCollections._analysisGroups", "dataCollections._analysisGroups.keyword"}

    if isinstance(node, dict):
        out: Dict[str, Any] = {}
        for k, v in node.items():
            # term / terms
            if k in ("term", "terms") and isinstance(v, dict):
                inner: Dict[str, Any] = {}
                for f, vv in v.items():
                    if f in target_fields:
                        if isinstance(vv, list):
                            inner[f] = [_normalise_ag_value(x) for x in vv]
                        elif isinstance(vv, dict) and "value" in vv:
                            tmp = dict(vv)
                            tmp["value"] = _normalise_ag_value(tmp["value"])
                            inner[f] = tmp
                        else:
                            inner[f] = _normalise_ag_value(vv)
                    else:
                        inner[f] = _normalise_ag_values_in_body(vv)
                out[k] = inner
            # aggs terms: normalise include values if present, keep field as-is
            elif k == "aggs" and isinstance(v, dict):
                aggs_out: Dict[str, Any] = {}
                for agg_name, agg_body in v.items():
                    if isinstance(agg_body, dict) and "terms" in agg_body and isinstance(agg_body["terms"], dict):
                        terms = dict(agg_body["terms"])
                        f = str(terms.get("field", ""))
                        if f in target_fields and "include" in terms and isinstance(terms["include"], list):
                            terms["include"] = [_normalise_ag_value(x) for x in terms["include"]]
                        aggs_out[agg_name] = {**agg_body, "terms": terms}
                    else:
                        aggs_out[agg_name] = _normalise_ag_values_in_body(agg_body)
                out[k] = aggs_out
            else:
                out[k] = _normalise_ag_values_in_body(v)
        return out
    if isinstance(node, list):
        return [_normalise_ag_values_in_body(x) for x in node]
    return node


# ---- Ensure ES returns requested fields + minimal _source for fallback -------

def _request_includes_ag_fields(es_body: Dict[str, Any]) -> bool:
    requested = es_body.get("fields") or []
    return isinstance(requested, list) and any(
        f in ("dataCollections._analysisGroups", "dataCollections._analysisGroups.keyword")
        for f in requested
    )


def _ensure_docvalues_for_requested_fields(es_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    If the FE asks for dataCollections._analysisGroups in 'fields', ensure doc values
    are requested so ES returns hits.hits[*].fields[...] .
    """
    if not _request_includes_ag_fields(es_body):
        return es_body
    dv = set(es_body.get("docvalue_fields") or [])
    dv.add("dataCollections._analysisGroups")
    es_body["docvalue_fields"] = list(dv)
    return es_body


def _ensure_min_source_for_ag_fallback(es_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    If FE asks for dataCollections._analysisGroups AND _source is False,
    include just enough _source so we can synthesise the values as a fallback.
    """
    if not _request_includes_ag_fields(es_body):
        return es_body

    needed = ["dataCollections.sequence", "dataCollections.alignment", "dataCollections.variants"]
    src = es_body.get("_source")
    if src is False:
        es_body["_source"] = {"includes": needed}
    elif isinstance(src, dict):
        inc = set(src.get("includes") or [])
        inc.update(needed)
        es_body["_source"]["includes"] = list(inc)
    # else: _source True or omitted → OK
    return es_body


def _synth_ag_from_source(src: Dict[str, Any]) -> List[str]:
    """Build analysis-group titles from _source dataCollections.* arrays (fallback)."""
    ags: List[str] = []
    for dc in src.get("dataCollections") or []:
        if not isinstance(dc, dict):
            continue
        for k in ("sequence", "alignment", "variants"):
            vals = dc.get(k) or []
            if isinstance(vals, list):
                ags.extend([v for v in vals if isinstance(v, str)])
    # de-duplicate, preserve order
    seen, out = set(), []
    for v in ags:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _expand_aliases(values: List[str]) -> List[str]:
    """
    Expand canonical titles to include aliases (code, shortTitle) so the FE's
    short column labels (e.g. 'ONT', 'Low cov WGS') match.
    """
    if not values:
        return values
    _, expansions = _get_ag_maps()
    seen, out = set(), []
    for v in values:
        # Expand v if it's a canonical (or already an alias); always include v
        aliases = expansions.get(v, [v])
        for a in aliases:
            if a not in seen:
                seen.add(a)
                out.append(a)
    return out


# ================================ Endpoints ===================================

@router.post("/_search")
def search_samples(body: Optional[Dict[str, Any]] = Body(None)) -> Dict[str, Any]:
    """
    POST /beta/sample/_search

    Compatibility tweaks:
      - size:-1 → capped to ES_ALL_SIZE_CAP
      - track_total_hits=True for exact totals
      - default sort by name.keyword asc (if none provided)
      - normalise analysis-group VALUES (codes → titles) but DO NOT rename fields
      - if 'fields' asks for dataCollections._analysisGroups, request doc values
        and ensure minimal _source for synthesis fallback
      - after search: if that field is requested, synthesise when missing and
        expand aliases so short FE labels also match
    """
    es_body: Dict[str, Any] = body or {"query": {"match_all": {}}}

    # "return all"
    size = es_body.get("size")
    if isinstance(size, int) and size < 0:
        es_body["size"] = settings.ES_ALL_SIZE_CAP

    # exact totals + stable default sort
    es_body.setdefault("track_total_hits", True)
    if "sort" not in es_body:
        es_body["sort"] = [{"name.keyword": {"order": "asc"}}]

    # FE 'data collection' filter: title.std → title.keyword
    es_body = _rewrite_dc_title_to_keyword(es_body)

    # Technology filters: normalise VALUES only (keep field name)
    es_body = _normalise_ag_values_in_body(es_body)

    # Ensure doc values + minimal _source for fallback if needed
    es_body = _ensure_docvalues_for_requested_fields(es_body)
    es_body = _ensure_min_source_for_ag_fallback(es_body)

    # Query ES
    try:
        resp = es.search(index=INDEX, body=es_body, ignore_unavailable=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # If FE requested analysis-group fields and ES didn't include them,
    # synthesise from _source and attach to hits[*].fields["dataCollections._analysisGroups"].
    wants_ag_fields = _request_includes_ag_fields(es_body)
    if wants_ag_fields:
        hits_list = ((resp.get("hits") or {}).get("hits") or [])
        for h in hits_list:
            fields = h.get("fields")
            if not isinstance(fields, dict):
                fields = {}
                h["fields"] = fields
            vals = fields.get("dataCollections._analysisGroups")
            if not isinstance(vals, list) or not vals:
                vals = fields.get("dataCollections._analysisGroups.keyword")
            if not isinstance(vals, list) or not vals:
                vals = _synth_ag_from_source(h.get("_source") or {})
            # Expand aliases so FE short labels (e.g. 'ONT') match too
            if isinstance(vals, list) and vals:
                fields["dataCollections._analysisGroups"] = _expand_aliases(vals)

    return normalise_es_response(resp)


# Dev convenience: confirm wiring quickly
@router.get("/_search")
def search_samples_get() -> Dict[str, Any]:
    return search_samples({"query": {"match_all": {}}, "size": 25})


@router.get("/{name}")
def get_sample(name: str = Path(..., description="Sample identifier (often the ES _id)")) -> Dict[str, Any]:
    """
    GET /beta/sample/{name}
    Shape matches FE expectations: { "_source": { ...Sample... } }
    """
    # 1) by ES _id (fast)
    try:
        doc = es.get(index=INDEX, id=name, ignore=[404])
        if doc and doc.get("found"):
            return {"_source": doc.get("_source", {})}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}") from e

    # 2) fallback: by unique name.keyword
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
      - fields: list of dotted _source paths (plus special "_id" / "_index")
      - column_names: optional header labels (same length as fields)
      - query: ES query (defaults to match_all)
      - size: integer (capped server-side)

    Response:
      - text/tab-separated-values
      - arrays joined by commas; tabs/newlines stripped
    """
    # Parse payload from (1) form field, or (2) raw JSON body, or (3) manual form parse
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

    # Cap export size
    size = payload.get("size")
    if not isinstance(size, int) or size < 0 or size > settings.ES_ALL_SIZE_CAP:
        size = settings.ES_ALL_SIZE_CAP

    # Normalise AG values in the export query (keep field name)
    query = _normalise_ag_values_in_body(query)

    # Query ES
    try:
        resp = es.search(
            index=INDEX,
            body={"query": query, "_source": True, "size": size, "track_total_hits": True},
            ignore_unavailable=True,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Elasticsearch error: {e}")

    hits = (resp.get("hits") or {}).get("hits") or []

    # Default fields if none provided
    if not fields:
        fields = ["_id", "name", "sex"]

    header = "\t".join(column_names) if column_names and len(column_names) == len(fields) else "\t".join(fields)

    # Build TSV
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