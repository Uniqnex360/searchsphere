from elasticsearch import Elasticsearch
from app.settings import settings

# 1. Create the client instance ONCE at the module level
# This acts as your connection pool singleton.
_es_client = Elasticsearch(
    settings.elastic_search_url,
    api_key=settings.elastic_search_api_key,
    max_retries=3,
    retry_on_timeout=True,
    headers={"accept": "application/vnd.elasticsearch+json;compatible-with=8"},
    sniff_on_start=False,
    sniff_on_node_failure=False,
    sniff_timeout=None,
    # ADDITION: Helps prevent the 28s timeout if a connection goes stale
    connections_per_node=10,
    request_timeout=240,
)


def get_es():
    """
    Dependency to get the ES client.
    FastAPI will call this for every request, but it
    always returns the same pre-warmed connection pool.
    """
    return _es_client
