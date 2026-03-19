from typing import List, Dict, Any
from elasticsearch import Elasticsearch

from app.services import ElasticsearchService
from app.helpers import get_embedding
from app.models import Product


async def sync_with_vector_product(
    service: ElasticsearchService,
    product_id: str | int,
    product: Product,
):
    """syncs product data with vector embeddings using background tasks"""

    try:
        product_name_em = get_embedding(product.product_name)
        product_description_em = get_embedding(product.long_description)
        res = service.upsert(
            product.id,
            data={
                "product_name": product.product_name,
                "long_description": product.long_description,
                "product_name_vector": product_name_em,
                "long_description_vector": product_description_em,
            },
        )
        print("ES response", res)
    except Exception as e:
        print(f"ES sync failed for {product_id}: {e}")


async def autocomplete_product_vector(
    es: Elasticsearch,
    query: str,
    size: int = 10,
    index: str = "product_vector",
) -> List[Dict[str, Any]]:
    """
    Keyword + identifier boosted search for products (Elasticsearch only).
    """

    body = {
        "size": size,
        "query": {
            "bool": {
                "should": [
                    # Main keyword search
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "product_name^5",
                                "brand^3",
                                "search_text^4",
                                "short_description^2",
                                "long_description^2",
                                "features",
                            ],
                        }
                    },
                    # Identifier exact match (high boost)
                    {"term": {"sku": {"value": query, "boost": 10}}},
                    {"term": {"mpn": {"value": query, "boost": 10}}},
                    {"term": {"gtin": {"value": query, "boost": 10}}},
                    {"term": {"upc": {"value": query, "boost": 10}}},
                    {"term": {"ean": {"value": query, "boost": 10}}},
                ],
                "minimum_should_match": 1,
            }
        },
    }

    response = es.search(index=index, body=body)

    hits = response.get("hits", {}).get("hits", [])

    results: List[Dict[str, Any]] = []

    for hit in hits:
        source = hit.get("_source", {})

        results.append(
            {
                "id": hit.get("_id"),
                "score": hit.get("_score"),
                "product_name": source.get("product_name"),
                "brand": source.get("brand"),
                "category": source.get("category"),
            }
        )

    return results
