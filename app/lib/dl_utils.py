# app/lib/dl_utils.py
from typing import Any, Dict, Iterable, List, Union
import json

JsonLike = Union[str, int, float, bool, None, List[Any], Dict[str, Any]]

def get_nested(source: Dict[str, Any], path: str) -> JsonLike:
    """
    Resolve a dotted `_source` path (e.g. "a.b.c") for TSV export.

    If a path segment hits a list of dicts, collect that child field from each
    item and drop empty values. Returns a scalar/list/dict ready for
    `to_tsv_cell()`.
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
    Serialize a value into a single TSV cell.

    - lists → join items with `sep`, skipping empties
    - dicts → compact JSON
    - scalars → str(value)
    Tabs/newlines are stripped to keep the file well-formed; None → "".
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
    Yield tab-separated lines for a TSV download from ES hits.

    `_id` and `_index` come from the hit; other columns are dotted paths
    resolved from `_source` via `get_nested()`, then formatted with `to_tsv_cell()`.
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