from elasticsearch import Elasticsearch
from app.settings import settings


def get_es_client():
    es = Elasticsearch(
        settings.elastic_search_url,
        request_timeout=10,
        max_retries=3,
        retry_on_timeout=True,
        headers={"accept": "application/vnd.elasticsearch+json;compatible-with=8"},
    )
    return es


es = get_es_client()


def get_es():
    """dependency to get es client"""

    return get_es_client()
