from elasticsearch import Elasticsearch
from app.core.config import settings


def build_es() -> Elasticsearch:
    auth_kwargs = {}
    if settings.ES_API_KEY:
        auth_kwargs["api_key"] = settings.ES_API_KEY
    elif settings.ES_USERNAME and settings.ES_PASSWORD:
        auth_kwargs["basic_auth"] = (settings.ES_USERNAME, settings.ES_PASSWORD)

    return Elasticsearch(
        settings.ES_HOST,
        retry_on_timeout=True,
        max_retries=3,
        http_compress=True,
        connections_per_node=10,
        **auth_kwargs,
    )


es = build_es()
