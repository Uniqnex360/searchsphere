import re
from typing import Dict, Any, List, Optional
from elasticsearch import Elasticsearch
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

from app.services import ElasticsearchService, QDrantCollection
from app.helpers import get_embedding
from app.models import Product


async def sync_product_with_es_qdrant(
    service: ElasticsearchService,
    product_id: str | int,
    product: Product,
    qdrant_client: QdrantClient,
):
    """syncs product data with elastic search and qdrand concurrently"""

    data = {
        # search
        "product_name": product.product_name,
        # search + filters
        "sku": product.sku,
        "mpn": product.mpn,
        "brand": product.brand,
        "gtin": product.gtin,
        "ean": product.ean,
        "upc": product.upc,
        "taxonomy": product.taxonomy,
        "country_of_origin": product.country_of_origin,
        "warranty": product.warranty,
        "industry_name": product.industry.industry_name if product.industry else None,
        "category_name": product.category.name if product.category else None,
        # float
        "weight": product.weight,
        "length": product.length,
        "width": product.width,
        "height": product.height,
        "base_price": product.base_price,
        "sale_price": product.sale_price,
        "selling_price": product.selling_price,
        "special_price": product.special_price,
        # str #need search only
        "weight_unit": product.weight_unit,
        "dimension_unit": product.dimension_unit,
        "currency": product.currency,
        "stock_status": product.stock_status,
        "vendor_name": product.vendor_name,
        "vendor_sku": product.vendor_sku,
        "short_description": product.short_description,
        "long_description": product.long_description,
        "meta_title": product.meta_title,
        "meta_description": product.meta_description,
        "search_keywords": product.search_keywords,
        # int
        "stock_qty": product.stock_qty,
        # objects
        "features": [{"name": f.name, "value": f.value} for f in product.features],
        "attributes": [
            {
                "name": attr.attribute_name,
                "value": attr.attribute_value,
                "uom": attr.attribute_uom,
            }
            for attr in product.attributes
        ],
        "images": [{"name": img.name, "url": img.url} for img in product.images],
        "videos": [{"name": vid.name, "url": vid.url} for vid in product.videos],
        "documents": [{"name": doc.name, "url": doc.url} for doc in product.documents],
    }

    service.upsert(product_id, data)

    # hadling qdrant
    qdrant_text = f"""
    Product: {product.product_name or ""}
    Features: {" ".join([f.value or "" for f in product.features])}
    Attributes: {" ".join([
        f"{attr.attribute_name or ''} {attr.attribute_value or ''} {attr.attribute_uom or ''}"
        for attr in product.attributes
    ])}
    Short Description: {product.short_description}
    Descriptoin: {product.long_description}
    """

    # print("text count", len(qdrant_text.split())) by the above text size is 200 t0 300 words

    vector = get_embedding(qdrant_text)

    point = PointStruct(
        id=product_id,
        vector=vector,
        payload=data,
    )

    qdrant_client.upsert(collection_name=QDrantCollection.PRODUCT.value, points=[point])

    return None


# -----------------------------
# 1. SIMPLE QUERY NORMALIZER
# -----------------------------
def normalize_query(q: str) -> str:
    if not q:
        return q

    q = q.lower().strip()
    return " ".join(q.split())


# -----------------------------
# 2. BUILD ELASTIC QUERY
# -----------------------------
def build_search_query(query: str) -> Dict[str, Any]:
    query = normalize_query(query)

    return {
        "bool": {
            "should": [
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
                        "fuzziness": "AUTO",
                        "prefix_length": 1,
                        "max_expansions": 50,
                        "operator": "and",
                    }
                },
                {"term": {"sku.keyword": {"value": query, "boost": 20}}},
                {"term": {"mpn.keyword": {"value": query, "boost": 20}}},
                {"term": {"gtin.keyword": {"value": query, "boost": 20}}},
                {"term": {"upc.keyword": {"value": query, "boost": 20}}},
                {"term": {"ean.keyword": {"value": query, "boost": 20}}},
            ],
            # "minimum_should_match": 1,
        }
    }


# -----------------------------
# 3. MULTI-SELECT FILTER ENGINE
# -----------------------------
def apply_filters(filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    must = []

    if not filters:
        return must

    # -------------------------
    # MULTI-SELECT BRAND
    # -------------------------
    if filters.get("brand"):
        must.append({"terms": {"brand.keyword": filters["brand"]}})

    # -------------------------
    # MULTI-SELECT CATEGORY
    # -------------------------
    if filters.get("category"):
        must.append({"terms": {"category_name.keyword": filters["category"]}})

    # -------------------------
    # PRICE RANGE
    # -------------------------
    price_range = {}

    if filters.get("price_min") is not None:
        price_range["gte"] = filters["price_min"]

    if filters.get("price_max") is not None:
        price_range["lte"] = filters["price_max"]

    if price_range:
        must.append({"range": {"base_price": price_range}})

    return must


# -----------------------------
# 4. VECTOR SEARCH (QDRANT)
# -----------------------------
def vector_search(qdrant, query: str, limit: int = 20, filters: dict = None):
    # -----------------------------
    # HANDLE EMPTY QUERY
    # -----------------------------
    if not query or not query.strip():
        return []  # ✅ skip vector search

    vector = get_embedding(query)
    from qdrant_client.models import Filter, FieldCondition, MatchValue

    qdrant_filter = None
    if filters:
        must_conditions = []
        if "brand" in filters and filters["brand"]:
            must_conditions.append(
                Filter(
                    should=[
                        FieldCondition(key="brand", match=MatchValue(value=b))
                        for b in filters["brand"]
                    ],
                )
            )
        if "category" in filters and filters["category"]:
            must_conditions.append(
                Filter(
                    should=[
                        FieldCondition(key="category", match=MatchValue(value=c))
                        for c in filters["category"]
                    ],
                )
            )

        if must_conditions:
            qdrant_filter = Filter(must=must_conditions)

    response = qdrant.query_points(
        collection_name=QDrantCollection.PRODUCT.value,
        query=vector,
        query_filter=qdrant_filter,
        limit=20,
        with_payload=True,
    )
    return response.points


from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch


# -----------------------------
# MERGE FUNCTION
# -----------------------------
def merge_results(keyword_results, vector_results, debug: bool = False):
    """
    Hybrid merge with debug logs to understand:
    - ES vs Vector contributions
    - Score composition
    """
    ES_WEIGHT = 0.7
    VECTOR_WEIGHT = 0.3
    results = {}

    # -----------------------------
    # DEBUG: Input Summary
    # -----------------------------
    if debug:
        print("\n========== MERGE DEBUG START ==========")
        print(f"ES Results Count      : {len(keyword_results)}")
        print(f"Vector Results Count  : {len(vector_results)}")

    # -----------------------------
    # STEP 1: Add Elasticsearch Results
    # -----------------------------
    if debug and keyword_results:
        print("\n--- Elasticsearch Results ---")

    for r in keyword_results:
        pid = r["_id"]
        es_score = r.get("_score") or 0
        if debug:
            print(
                f"[ES] ID={pid} | ES Score={es_score:.4f} | Weighted={es_score * ES_WEIGHT:.4f}"
            )

        results[pid] = {
            "data": r["_source"],
            "score": es_score * ES_WEIGHT,
            "score_breakdown": {"es": es_score, "vector": 0},
            "es_order": r.get("_es_order", 0),  # preserve ES order
        }

    # -----------------------------
    # STEP 2: Merge Vector Results
    # -----------------------------
    if debug and vector_results:
        print("\n--- Vector Results ---")

    for r in vector_results:
        pid = str(r.id)
        vector_score = r.score

        if pid not in results:
            # Vector-only result
            if debug:
                print(
                    f"[VECTOR ONLY] ID={pid} | Vector Score={vector_score:.4f} | Weighted={vector_score * VECTOR_WEIGHT:.4f}"
                )

            results[pid] = {
                "data": r.payload,
                "score": vector_score * VECTOR_WEIGHT,
                "score_breakdown": {"es": 0, "vector": vector_score},
                "es_order": float("inf"),  # comes after ES results
            }
        else:
            # Exists in both ES + Vector
            if debug:
                print(
                    f"[MERGED] ID={pid} | Vector Score={vector_score:.4f} | Added={vector_score * VECTOR_WEIGHT:.4f}"
                )

            results[pid]["score"] += vector_score * VECTOR_WEIGHT
            results[pid]["score_breakdown"]["vector"] = vector_score

    # -----------------------------
    # DEBUG: Final Scores
    # -----------------------------
    if debug:
        print("\n--- Final Merged Results ---")
        for pid, item in results.items():
            print(
                f"[FINAL] ID={pid} | "
                f"ES={item['score_breakdown']['es']:.4f} | "
                f"Vector={item['score_breakdown']['vector']:.4f} | "
                f"Final Score={item['score']:.4f}"
            )
        print("========== MERGE DEBUG END ==========\n")

    return results


# -----------------------------
# FINAL HYBRID SEARCH API
# -----------------------------
async def autocomplete_with_es_qdrant(
    es: Elasticsearch,
    qdrant,
    query: str,
    size: int = 50,
    filters: Optional[Dict[str, Any]] = None,
    sort_by: str = "relevance",  # "relevance" | "product_name" | "base_price"
    sort_order: str = "desc",  # "desc" | "asc"
    index: str = "product_vector",
) -> List[Dict[str, Any]]:

    if not query.strip():
        base_query = {"match_all": {}}
    else:
        base_query = build_search_query(query)
    filter_clauses = apply_filters(filters)

    # -----------------------------
    # Elasticsearch Sort Handling
    # -----------------------------
    es_sort = []
    # Determine a very large number that won't appear in real data
    VERY_HIGH = 1e10
    VERY_LOW = -1e10
    if sort_by == "product_name":
        es_sort.append(
            {
                "product_name.keyword": {
                    "order": sort_order,
                    "missing": "_last",  # nulls go last
                }
            }
        )
    elif sort_by == "base_price":
        missing_value = VERY_HIGH if sort_order == "asc" else VERY_LOW
        es_sort.append({"base_price": {"order": sort_order, "missing": missing_value}})
    else:
        es_sort.append({"_score": {"order": sort_order}})

    # -----------------------------
    # ELASTIC QUERY
    # -----------------------------
    body = {
        "size": size,
        "_source": [
            "product_name",
            "brand",
            "category_name",
            "taxonomy",
            "base_price",
            "images.url",
        ],
        "query": {
            "bool": {
                "must": [base_query],
                "filter": filter_clauses,
            }
        },
        "sort": es_sort,
    }

    es_response = es.search(index=index, body=body)
    keyword_hits = es_response.get("hits", {}).get("hits", [])

    # -----------------------------
    # VECTOR SEARCH
    # -----------------------------
    vector_results = vector_search(qdrant, query, limit=size, filters=filters)
    # -----------------------------
    # MERGE RESULTS
    # -----------------------------
    merged = merge_results(keyword_hits, vector_results)

    # -----------------------------
    # PREPARE FINAL RESULTS
    # -----------------------------
    results = []
    for pid, item in merged.items():
        source = item["data"]
        results.append(
            {
                "id": pid,
                "score": item["score"],
                "name": source.get("product_name"),
                "brand": source.get("brand"),
                "category": source.get("category_name") or source.get("category"),
                "base_price": source.get("base_price"),
                "images": [
                    i.get("url")
                    for i in (source.get("images") or [])
                    if isinstance(i, dict)
                ],
                "es_order": item.get("es_order", float("inf")),
            }
        )

    # -----------------------------
    # SORT FINAL RESULTS (null-safe, tie-breaker with ES order)
    # -----------------------------
    reverse = sort_order == "desc"

    def null_safe_key(val, reverse=False):
        # None always sorts last
        if val is None:
            return float("inf") if not reverse else float("-inf")
        return val

    if sort_by == "product_name":
        results.sort(
            key=lambda x: (
                null_safe_key((x.get("name") or "").lower(), reverse),
                x["es_order"],
            ),
            reverse=reverse,
        )
    elif sort_by == "base_price":
        results.sort(
            key=lambda x: (null_safe_key(x.get("base_price"), reverse), x["es_order"]),
            reverse=reverse,
        )
    else:
        results.sort(
            key=lambda x: (null_safe_key(x.get("score"), reverse), x["es_order"]),
            reverse=reverse,
        )

    # Remove ES order from final output
    for r in results:
        r.pop("es_order", None)

    return results
