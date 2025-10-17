from elasticsearch import Elasticsearch
from app.core.config import settings

def build_es() -> Elasticsearch:
    auth_kwargs = {}
    if settings.ES_API_KEY:
        auth_kwargs["api_key"] = settings.ES_API_KEY
    elif settings.ES_USERNAME and settings.ES_PASSWORD:
        auth_kwargs["basic_auth"] = (settings.ES_USERNAME, settings.ES_PASSWORD)

    common_kwargs = dict(
        retry_on_timeout=True,
        max_retries=3,
        http_compress=True,
        connections_per_node=10,
        request_timeout=30,
        **auth_kwargs,
    )

    # Prefer Cloud ID if supplied
    if settings.ES_CLOUD_ID:
        return Elasticsearch(cloud_id=settings.ES_CLOUD_ID, **common_kwargs)

    # Otherwise fall back to a direct host/URL (for local dev)
    if settings.ES_HOST:
        return Elasticsearch(settings.ES_HOST, **common_kwargs)

    # Nothing configured
    raise RuntimeError(
        "No Elasticsearch connection configured. Set ES_CLOUD_ID or ES_HOST (+ credentials)."
    )

es = build_es()