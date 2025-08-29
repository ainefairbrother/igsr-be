from elasticsearch import Elasticsearch
from ..core.config import settings

def search_samples(es: Elasticsearch, q: str | None, page: int, page_size: int):
    index = settings.INDEX_SAMPLES
    query = {"match_all": {}} if not q else {"simple_query_string": {"query": q}}

    resp = es.search(
        index=index,
        query=query,
        from_=(page - 1) * page_size,
        size=page_size,
    )

    hits = resp["hits"]["hits"]
    total_field = resp["hits"]["total"]
    total = total_field["value"] if isinstance(total_field, dict) else int(total_field)

    items = [{"id": h["_id"], **(h.get("_source") or {})} for h in hits]
    return items, int(total)