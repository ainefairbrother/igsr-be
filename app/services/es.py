# app/services/es.py
from elasticsearch import Elasticsearch
from app.core.config import settings


def _build_client() -> Elasticsearch:
    kwargs = {
        "retry_on_timeout": True,  # retry if a request times out
        "max_retries": 3,          # small number to avoid long stalls
        "request_timeout": 10,     # seconds
    }

    # API key takes precedence if provided, else fall back to basic auth
    if settings.ES_API_KEY:
        kwargs["api_key"] = settings.ES_API_KEY
    elif settings.ES_USERNAME and settings.ES_PASSWORD:
        kwargs["basic_auth"] = (settings.ES_USERNAME, settings.ES_PASSWORD)

    return Elasticsearch(hosts=[settings.ES_HOST], **kwargs)


es: Elasticsearch = _build_client()