# app/services/es.py
from elasticsearch import Elasticsearch
from app.core.config import settings

def make_es() -> Elasticsearch:
    kwargs = {}
    if settings.ES_API_KEY:
        kwargs["api_key"] = settings.ES_API_KEY
    elif settings.ES_USERNAME and settings.ES_PASSWORD:
        kwargs["basic_auth"] = (settings.ES_USERNAME, settings.ES_PASSWORD)

    # ES python client v8 works fine against recent clusters
    return Elasticsearch(settings.ES_HOST, **kwargs)

es = make_es()