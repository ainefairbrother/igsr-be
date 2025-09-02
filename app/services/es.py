# app/services/es.py
from __future__ import annotations

from elasticsearch import Elasticsearch
from app.core.config import settings


def _build_client() -> Elasticsearch:
    """
    Construct a sync Elasticsearch client with sensible defaults:
      - small retry budget + retry on timeout
      - default request timeout via .options(...), when supported
      - HTTP compression on
      - API key takes precedence over basic auth
      - supports comma-separated ES_HOST list
    """
    # Allow "http://host1:9200, http://host2:9200"
    hosts = [h.strip() for h in str(settings.ES_HOST).split(",") if h.strip()]

    kwargs = {
        "retry_on_timeout": True,
        "max_retries": 3,
        "http_compress": True,
        "connections_per_node": 10,
    }

    # Auth: API key > basic auth
    if settings.ES_API_KEY:
        kwargs["api_key"] = settings.ES_API_KEY
    elif settings.ES_USERNAME and settings.ES_PASSWORD:
        kwargs["basic_auth"] = (settings.ES_USERNAME, settings.ES_PASSWORD)

    client = Elasticsearch(hosts=hosts, **kwargs)

    # Set a default per-request timeout if the client supports .options()
    try:
        client = client.options(request_timeout=10)  # seconds
    except AttributeError:
        # Older client: no .options(). If needed, pass request_timeout=... per call.
        pass

    return client


es: Elasticsearch = _build_client()