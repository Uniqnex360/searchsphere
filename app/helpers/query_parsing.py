import json
import redis
import re
from elasticsearch import Elasticsearch
from typing import Dict, List, Any

REDIS_KEY = "product:taxonomy:cache"

redis_client = redis.Redis(
    host="redis",
    #host="localhost",
    port=6379,
    decode_responses=True,
)


# -----------------------------
# 1. FETCH UNIQUE TAXONOMY FROM ELASTICSEARCH
# -----------------------------
async def fetch_taxonomy_from_es(
    es: Elasticsearch, index: str = "product_v7"
) -> Dict[str, List[str]]:
    body = {
        "size": 0,
        "aggs": {
            "brands": {"terms": {"field": "brand.keyword", "size": 10000}},
            "categories": {"terms": {"field": "category.keyword", "size": 10000}},
            "product_types": {
                "terms": {"field": "product_type.keyword", "size": 10000}
            },
        },
    }

    res = es.search(index=index, body=body)

    return {
        "brands": [b["key"] for b in res["aggregations"]["brands"]["buckets"]],
        "categories": [c["key"] for c in res["aggregations"]["categories"]["buckets"]],
        "product_types": [
            p["key"] for p in res["aggregations"]["product_types"]["buckets"]
        ],
    }


# -----------------------------
# 2. LOAD TAXONOMY INTO REDIS (CACHE FOREVER)
# -----------------------------
async def load_taxonomy(es: Elasticsearch):
    if redis_client.exists(REDIS_KEY):
        return

    taxonomy = await fetch_taxonomy_from_es(es)

    # store as JSON (safe)
    redis_client.set(REDIS_KEY, json.dumps(taxonomy))


# -----------------------------
# 3. GET TAXONOMY FROM REDIS
# -----------------------------
def get_taxonomy() -> Dict[str, List[str]]:
    data = redis_client.get(REDIS_KEY)

    if not data:
        return {"brands": [], "categories": [], "product_types": []}

    return json.loads(data)


# -----------------------------
# 4. MATCH QUERY AGAINST TAXONOMY (FIXED)
# -----------------------------
def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9\s]", " ", text.lower())


def _is_whole_match(term: str, query: str) -> bool:
    pattern = r"\b" + re.escape(term.lower()) + r"\b"
    return re.search(pattern, query.lower()) is not None


def match_query(query: str, taxonomy: Dict[str, List[str]]) -> Dict[str, Any]:
    q = query.lower()

    matched_brands = [b for b in taxonomy["brands"] if _is_whole_match(b, q)]

    matched_categories = [c for c in taxonomy["categories"] if _is_whole_match(c, q)]

    matched_product_types = [
        p for p in taxonomy["product_types"] if _is_whole_match(p, q)
    ]

    return {
        "brand": matched_brands,
        "category": matched_categories,
        "product_type": matched_product_types,
        "original_query": query,
    }


# -----------------------------
# 5. MAIN PARSE FUNCTION
# -----------------------------
async def parse_query(q: str, es: Elasticsearch) -> Dict[str, Any]:
    """
    Main function:
    - loads taxonomy into redis (if not present)
    - reads from redis
    - matches query
    """

    await load_taxonomy(es)

    taxonomy = get_taxonomy()

    return match_query(q, taxonomy)
