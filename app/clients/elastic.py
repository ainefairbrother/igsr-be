from elasticsearch import Elasticsearch
from app.core.config import settings

def get_es() -> Elasticsearch:
    # Prefer API key in production (no username/password in logs)
    if settings.ES_API_KEY:
        return Elasticsearch(settings.ES_HOST, api_key=settings.ES_API_KEY, request_timeout=30)
    if settings.ES_USERNAME and settings.ES_PASSWORD:
        return Elasticsearch(settings.ES_HOST, basic_auth=(settings.ES_USERNAME, settings.ES_PASSWORD), request_timeout=30)
    return Elasticsearch(settings.ES_HOST, request_timeout=30)