from typing import List, Dict, Any
from elasticsearch import Elasticsearch
from app.services import ElasticsearchService
from app.models import Product


async def sync_with_product(
    service: ElasticsearchService,
    product_id: str | int,
    product: Product,
):
    """syncs product data with elastic search using background tasks"""

    try:
        service.upsert(
            product_id,
            data={
                "sku": product.sku,
                "product_name": product.product_name,
                "brand": product.brand,
                "mpn": product.mpn,
                "gtin": product.gtin,
                "ean": product.ean,
                "upc": product.upc,
                "country_of_origin": product.country_of_origin,
                "warranty": product.warranty,
                "weight": product.weight,  # float
                "weight_unit": product.weight_unit,  # str
                "length": product.length,  # float
                "width": product.width,  # float
                "height": product.height,  # float
                "dimension_unit": product.dimension_unit,  # str
                "currency": product.currency,  # str
                "base_price": product.base_price,  # float
                "sale_price": product.sale_price,  # float
                "selling_price": product.selling_price,  # float
                "special_price": product.special_price,  # float
                "taxnomy": product.taxonomy,  # str ex l1 > l2 > l3
                "stock_qty": product.stock_qty,  # int
                "stock_status": product.stock_status,  # str
                "vendor_name": product.vendor_name,  # str
                "vendor_sku": product.vendor_sku,
                "short_description": product.short_description,
                "long_description": product.long_description,
                "meta_title": product.meta_title,
                "meta_description": product.meta_description,
                "search_keywords": product.search_keywords,
                "certification": product.certification,
                "safety_standard": product.safety_standard,
                "hazardous_material": product.hazardous_material,
                "prop65_warning": product.prop65_warning,
                # fk and relationships
                "category_name": product.category.name if product.category else None,
                "industry_name": (
                    product.industry.industry_name if product.industry else None
                ),
                "features": [
                    {"name": f.name, "value": f.value} for f in product.features
                ],
                "attributes": [
                    {
                        "name": attr.attribute_name,
                        "value": attr.attribute_value,
                        "uom": attr.attribute_uom,
                    }
                    for attr in product.attributes
                ],
            },
        )
    except Exception as e:
        print(f"ES sync failed for {product_id}: {e}")


# async def autocomplete_products(
#     es: Elasticsearch, query: str, filters: dict | None = None
# ):
#     filters = filters or {}
#     must_filters = []

#     # -------------------------
#     # 🔥 ALL FILTERS (A → Z)
#     # -------------------------

#     if filters.get("brand"):
#         must_filters.append({"term": {"brand.keyword": filters["brand"]}})

#     if filters.get("category"):
#         must_filters.append({"term": {"category_name.keyword": filters["category"]}})

#     if filters.get("industry"):
#         must_filters.append({"term": {"industry_name.keyword": filters["industry"]}})

#     if filters.get("vendor_name"):
#         must_filters.append({"term": {"vendor_name.keyword": filters["vendor_name"]}})

#     if filters.get("stock_status"):
#         must_filters.append({"term": {"stock_status.keyword": filters["stock_status"]}})

#     if filters.get("currency"):
#         must_filters.append({"term": {"currency.keyword": filters["currency"]}})

#     if filters.get("country_of_origin"):
#         must_filters.append(
#             {"term": {"country_of_origin.keyword": filters["country_of_origin"]}}
#         )

#     if filters.get("warranty"):
#         must_filters.append({"term": {"warranty.keyword": filters["warranty"]}})

#     if filters.get("certification"):
#         must_filters.append(
#             {"term": {"certification.keyword": filters["certification"]}}
#         )

#     if filters.get("safety_standard"):
#         must_filters.append(
#             {"term": {"safety_standard.keyword": filters["safety_standard"]}}
#         )

#     # 🔥 Price filter
#     if filters.get("min_price") or filters.get("max_price"):
#         price = {"range": {"selling_price": {}}}
#         if filters.get("min_price"):
#             price["range"]["selling_price"]["gte"] = filters["min_price"]
#         if filters.get("max_price"):
#             price["range"]["selling_price"]["lte"] = filters["max_price"]
#         must_filters.append(price)

#     # 🔥 Weight filter
#     if filters.get("min_weight") or filters.get("max_weight"):
#         weight = {"range": {"weight": {}}}
#         if filters.get("min_weight"):
#             weight["range"]["weight"]["gte"] = filters["min_weight"]
#         if filters.get("max_weight"):
#             weight["range"]["weight"]["lte"] = filters["max_weight"]
#         must_filters.append(weight)

#     # 🔥 Nested FEATURE filter
#     if filters.get("features"):
#         must_filters.append(
#             {
#                 "nested": {
#                     "path": "features",
#                     "query": {
#                         "bool": {
#                             "must": [
#                                 {
#                                     "term": {
#                                         "features.name.keyword": filters["features"][
#                                             "name"
#                                         ]
#                                     }
#                                 },
#                                 {
#                                     "term": {
#                                         "features.value.keyword": filters["features"][
#                                             "value"
#                                         ]
#                                     }
#                                 },
#                             ]
#                         }
#                     },
#                 }
#             }
#         )

#     # 🔥 Nested ATTRIBUTE filter
#     if filters.get("attributes"):
#         must_filters.append(
#             {
#                 "nested": {
#                     "path": "attributes",
#                     "query": {
#                         "bool": {
#                             "must": [
#                                 {
#                                     "term": {
#                                         "attributes.name.keyword": filters[
#                                             "attributes"
#                                         ]["name"]
#                                     }
#                                 },
#                                 {
#                                     "term": {
#                                         "attributes.value.keyword": filters[
#                                             "attributes"
#                                         ]["value"]
#                                     }
#                                 },
#                             ]
#                         }
#                     },
#                 }
#             }
#         )

#     # -------------------------
#     # 🔥 MAIN SEARCH (ALL FIELDS)
#     # -------------------------

#     response = es.search(
#         index="products",
#         body={
#             "size": 10,
#             "_source": True,
#             "query": {
#                 "bool": {
#                     "must": must_filters,
#                     "should": [
#                         # 🔥 1. Exact phrase match (highest quality)
#                         {
#                             "multi_match": {
#                                 "query": query,
#                                 "type": "phrase",
#                                 "fields": [
#                                     "product_name^12",
#                                     "brand^10",
#                                     "category_name^8",
#                                     "industry_name^7",
#                                     "vendor_name^6",
#                                     "search_keywords^9",
#                                 ],
#                             }
#                         },
#                         # 🔥 2. Prefix (autocomplete)
#                         {
#                             "multi_match": {
#                                 "query": query,
#                                 "type": "bool_prefix",
#                                 "fields": [
#                                     # core
#                                     "product_name^12",
#                                     "product_name._2gram",
#                                     "product_name._3gram",
#                                     "brand^10",
#                                     "category_name^8",
#                                     "industry_name^7",
#                                     "vendor_name^6",
#                                     "vendor_sku^6",
#                                     "search_keywords^9",
#                                     # descriptions
#                                     "short_description^4",
#                                     "long_description^2",
#                                     # metadata
#                                     "taxonomy^5",
#                                     "country_of_origin^4",
#                                     "warranty^3",
#                                     "certification^3",
#                                     "safety_standard^3",
#                                     "hazardous_material^2",
#                                     "prop65_warning^2",
#                                 ],
#                             }
#                         },
#                         # 🔥 3. Fuzzy (typo tolerance)
#                         {
#                             "multi_match": {
#                                 "query": query,
#                                 "fields": [
#                                     "product_name^12",
#                                     "brand^10",
#                                     "category_name^8",
#                                     "vendor_name^6",
#                                 ],
#                                 "fuzziness": "AUTO",
#                             }
#                         },
#                         # 🔥 4. DESCRIPTION deep search
#                         {
#                             "multi_match": {
#                                 "query": query,
#                                 "type": "best_fields",
#                                 "fields": ["short_description^4", "long_description^2"],
#                             }
#                         },
#                         # 🔥 5. FEATURES (nested)
#                         # {
#                         #     "nested": {
#                         #         "path": "features",
#                         #         "query": {
#                         #             "multi_match": {
#                         #                 "query": query,
#                         #                 "fields": ["features.name", "features.value"],
#                         #                 "boost": 4,
#                         #             }
#                         #         },
#                         #     }
#                         # },
#                         {
#                             "multi_match": {
#                                 "query": query,
#                                 "fields": ["features.name", "features.value"],
#                                 "boost": 4,
#                             }
#                         },
#                         # 🔥 6. ATTRIBUTES (nested)
#                         # {
#                         #     "nested": {
#                         #         "path": "attributes",
#                         #         "query": {
#                         #             "multi_match": {
#                         #                 "query": query,
#                         #                 "fields": [
#                         #                     "attributes.name",
#                         #                     "attributes.value",
#                         #                     "attributes.uom",
#                         #                 ],
#                         #                 "boost": 4,
#                         #             }
#                         #         },
#                         #     }
#                         # },
#                         {
#                             "multi_match": {
#                                 "query": query,
#                                 "fields": [
#                                     "attributes.name",
#                                     "attributes.value",
#                                     "attributes.uom",
#                                 ],
#                                 "boost": 4,
#                             }
#                         },
#                     ],
#                     "minimum_should_match": 1,
#                 }
#             },
#             # -------------------------
#             # 🔥 SORTING
#             # -------------------------
#             # "sort": [
#             #     "_score",
#             #     {"stock_qty": "desc", "unmapped_type": "integer"},
#             #     {"selling_price": "asc", "unmapped_type": "float"},
#             # ],
#             # -------------------------
#             # 🔥 FACETS (FILTER UI)
#             # -------------------------
#             "aggs": {
#                 "brands": {"terms": {"field": "brand.keyword"}},
#                 "categories": {"terms": {"field": "category_name.keyword"}},
#                 "industries": {"terms": {"field": "industry_name.keyword"}},
#                 "stock": {"terms": {"field": "stock_status.keyword"}},
#             },
#         },
#     )

#     return response


from typing import List, Dict, Any
from elasticsearch import Elasticsearch


async def autocomplete_products(
    es: Elasticsearch, query: str, size: int = 10
) -> List[Dict[str, Any]]:
    """
    Autocomplete across all product-related fields

    Features:
    - Multi-field search (product, brand, category, etc.)
    - Prefix matching (autocomplete)
    - Fuzzy matching (typo tolerance)
    - Features & attributes search
    - Safe handling
    """

    # ✅ 1. Validate input
    if not query or not query.strip():
        return []

    query = query.strip()

    try:
        response = es.search(
            index="products",
            body={
                "size": size,
                "_source": [
                    "doc.product_name",
                    "doc.brand",
                    "doc.category_name",
                    "doc.vendor_name",
                    "doc.mpn",
                    "doc.sku",
                ],
                "query": {
                    "bool": {
                        "should": [
                            # 🔥 0. Combined fields (BEST for merged words like appleapple)
                            {
                                "combined_fields": {
                                    "query": query,
                                    "fields": [
                                        "doc.product_name^10",
                                        "doc.brand^8",
                                        "doc.category_name^5",
                                        "doc.vendor_name^4",
                                        "doc.industry_name^3",
                                        "doc.mpn^3",
                                        "doc.sku^3",
                                    ],
                                    "operator": "or",
                                }
                            },
                            # 🔥 1. Phrase prefix for ALL key fields
                            {
                                "multi_match": {
                                    "query": query,
                                    "type": "phrase_prefix",
                                    "fields": [
                                        "doc.product_name^10",
                                        "doc.brand^8",
                                        "doc.category_name^5",
                                        "doc.vendor_name^4",
                                        "doc.industry_name^3", #TODO: add all the fields
                                    ],
                                    "boost": 8,
                                }
                            },
                            # 🔥 2. Exact phrase
                            {
                                "multi_match": {
                                    "query": query,
                                    "type": "phrase",
                                    "fields": [
                                        "doc.product_name^6",
                                        "doc.brand^10",
                                        "doc.category_name^5",
                                        "doc.vendor_name^4",
                                    ],
                                }
                            },
                            # 🔥 3. Prefix autocomplete (main)
                            {
                                "multi_match": {
                                    "query": query,
                                    "type": "bool_prefix",
                                    "fields": [
                                        "doc.product_name^10",
                                        "doc.brand^6",
                                        "doc.category_name^5",
                                        "doc.vendor_name^4",
                                        "doc.industry_name^3",
                                        "doc.mpn^3",
                                        "doc.sku^3",
                                        "doc.taxnomy^2",
                                        "doc.long_description",
                                    ],
                                }
                            },
                            # 🔥 4. Fuzzy match
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "doc.product_name^6",
                                        "doc.brand^4",
                                        "doc.category_name^3",
                                        "doc.vendor_name^3",
                                    ],
                                    "fuzziness": "AUTO",
                                    "boost": 2,
                                }
                            },
                            # 🔥 5. Features
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "doc.features.name",
                                        "doc.features.value",
                                    ],
                                    "boost": 1.5,
                                }
                            },
                            # 🔥 6. Attributes
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "doc.attributes.name",
                                        "doc.attributes.value",
                                        "doc.attributes.uom",
                                    ],
                                    "boost": 1.5, #TODO: sort by alpha , price, and filter by brands
                                }
                            },
                            # 🔥 7. Exact keyword (IDs)
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "doc.mpn.keyword^10",
                                        "doc.sku.keyword^10",
                                    ],
                                }
                            },
                        ],
                        "minimum_should_match": 1,
                    }
                },
            },
        )

        # ✅ 2. Parse response safely
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            source = hit.get("_source", {}).get("doc", {})

            results.append(
                {
                    "id": hit.get("_id"),
                    "name": source.get("product_name"),
                    "brand": source.get("brand"),
                    "category": source.get("category_name"),
                    "vendor": source.get("vendor_name"),
                    "mpn": source.get("mpn"),
                    "sku": source.get("sku"),
                    "score": hit.get("_score"),  # useful for debugging
                }
            )

        return results

    except Exception as e:
        print("Elasticsearch autocomplete error:", str(e))
        return []
