from typing import Optional, List, Dict, Any
from elasticsearch import Elasticsearch
from sqlalchemy.orm import selectinload
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from app.models import Product, Brand
from app.services import ElasticsearchService, ESCollection, ElasticsearchIndexManager


async def create_or_get_index_v6(es: Elasticsearch, index_name: str, index_type: str):
    """
    index_type: "autosuggest" | "product"
    """

    index_service = ElasticsearchIndexManager(es)

    # Common settings (reuse)
    settings = {
        "analysis": {
            "analyzer": {
                "lowercase_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                }
            }
        }
    }

    # Dynamic mapping based on type
    if index_type == ESCollection.PRODUCT_AUTO_SUGGEST_V7.value:
        mappings = {
            "properties": {
                "brand_name": {
                    "type": "text",
                    "analyzer": "lowercase_analyzer",
                    "fields": {
                        "autocomplete": {
                            "type": "search_as_you_type",
                            "analyzer": "lowercase_analyzer",
                        }
                    },
                },
                "brand_category": {
                    "type": "text",
                    "analyzer": "lowercase_analyzer",
                    "fields": {
                        "autocomplete": {
                            "type": "search_as_you_type",
                            "analyzer": "lowercase_analyzer",
                        }
                    },
                },
                "brand_category_product_type": {
                    "type": "text",
                    "analyzer": "lowercase_analyzer",
                    "fields": {
                        "autocomplete": {
                            "type": "search_as_you_type",
                            "analyzer": "lowercase_analyzer",
                        }
                    },
                },
            }
        }

    elif index_type == ESCollection.PRODUCT_V7.value:
        mappings = {
            "properties": {
                "suggest": {
                    "type": "text",
                    "analyzer": "lowercase_analyzer",
                    "fields": {"keyword": {"type": "keyword"}},
                }
            }
        }

    else:
        raise ValueError("Invalid index_type")

    body = {"settings": settings, "mappings": mappings}

    index_service.ensure_index(index_name, body)

    return index_name


# async def sync_product_suggest_data_es_v6(
#     es: Elasticsearch, session: AsyncSession, batch_size: int = 100
# ) -> dict:
#     """
#     Sync product + autosuggest data using offset-based batching
#     """

#     autosuggest_index = ESCollection.PRODUCT_AUTO_SUGGEST_V7.value
#     product_index = ESCollection.PRODUCT_V7.value

#     autosuggest_service = ElasticsearchService(es, autosuggest_index)
#     product_service = ElasticsearchService(es, product_index)

#     # Ensure indices exist
#     await create_or_get_index_v6(es, autosuggest_index, autosuggest_index)
#     await create_or_get_index_v6(es, product_index, product_index)

#     offset = 0
#     total_processed = 0
#     batch_number = 1

#     print("🚀 Starting sync...")

#     while True:
#         print(f"\n📦 Fetching batch {batch_number} (offset={offset})")

#         result = await session.execute(
#             select(Product)
#             .options(
#                 selectinload(Product.brand),
#                 selectinload(Product.category),
#                 selectinload(Product.images),
#                 selectinload(Product.features),
#                 selectinload(Product.attributes),
#                 selectinload(Product.videos),
#                 selectinload(Product.documents),
#                 selectinload(Product.category),
#                 selectinload(Product.industry),
#                 selectinload(Product.product_type),
#             )
#             .order_by(Product.id)  # ⚠️ IMPORTANT for consistent pagination
#             .offset(offset)
#             .limit(batch_size)
#         )

#         products = result.scalars().all()

#         if not products:
#             print("✅ No more data to process.")
#             break

#         autosuggest_actions = []
#         product_actions = []

#         for product in products:

#             data = {
#                 # search
#                 "product_name": product.product_name,
#                 # identifiers
#                 "sku": product.sku,
#                 "mpn": product.mpn,
#                 "gtin": product.gtin,
#                 "ean": product.ean,
#                 "upc": product.upc,
#                 # relations (🔥 updated)
#                 "product_type": (
#                     product.product_type.product_type if product.product_type else None
#                 ),
#                 "industry_name": (
#                     product.industry.industry_name if product.industry else None
#                 ),
#                 "category": product.category.name if product.category else None,
#                 "category_name": product.category.name if product.category else None,
#                 # misc
#                 "taxonomy": product.taxonomy,
#                 "country_of_origin": product.country_of_origin,
#                 "warranty": product.warranty,
#                 # numeric
#                 "weight": product.weight,
#                 "length": product.length,
#                 "width": product.width,
#                 "height": product.height,
#                 "base_price": product.base_price,
#                 "sale_price": product.sale_price,
#                 "selling_price": product.selling_price,
#                 "special_price": product.special_price,
#                 # units
#                 "weight_unit": product.weight_unit,
#                 "dimension_unit": product.dimension_unit,
#                 "currency": product.currency,
#                 "stock_status": product.stock_status,
#                 # vendor
#                 "vendor_name": product.vendor_name,
#                 "vendor_sku": product.vendor_sku,
#                 # descriptions
#                 "short_description": product.short_description,
#                 "long_description": product.long_description,
#                 "meta_title": product.meta_title,
#                 "meta_description": product.meta_description,
#                 "search_keywords": product.search_keywords,
#                 # stock
#                 "stock_qty": product.stock_qty,
#                 # nested
#                 "features": [
#                     {"name": f.name, "value": f.value} for f in product.features
#                 ],
#                 "attributes": [
#                     {
#                         "name": attr.attribute_name,
#                         "value": attr.attribute_value,
#                         "uom": attr.attribute_uom,
#                     }
#                     for attr in product.attributes
#                 ],
#                 "images": [
#                     {"name": img.name, "url": img.url} for img in product.images
#                 ],
#                 "videos": [
#                     {"name": vid.name, "url": vid.url} for vid in product.videos
#                 ],
#                 "documents": [
#                     {"name": doc.name, "url": doc.url} for doc in product.documents
#                 ],
#             }

#             brand = product.brand
#             category = product.category
#             product_type_obj = product.product_type
#             attributes = product.attributes

#             if not brand or not category or not category.name or not product_type_obj:
#                 continue

#             # -------- Build basic suggestions --------
#             new_entry = f"{brand.brand_name} {category.name}"
#             product_type_entry = (
#                 f"{brand.brand_name} {category.name} {product_type_obj.product_type}"
#             )

#             suggest_set = set()
#             suggest_set.add(brand.brand_name)
#             suggest_set.add(new_entry)
#             suggest_set.add(product_type_entry)
#             suggest_set.add(product.product_name)
#             suggest_set.add(product.mpn)
#             suggest_set.add(product.sku)


#             # -------- Build attribute-based suggestions --------
#             attribute_entries = []
#             for attr in attributes:
#                 if attr.attribute_name and attr.attribute_value:
#                     attr_str = f"{attr.attribute_name} {attr.attribute_value}"
#                     full_entry = f"{brand.brand_name} {category.name} {product_type_obj.product_type} {attr_str}"
#                     attribute_entries.append(full_entry)
#                     suggest_set.add(full_entry)  # also add to product suggest set

#             # -------- Brand update for autosuggest --------
#             autosuggest_actions.append(
#                 {
#                     "_op_type": "update",
#                     "_id": brand.id,
#                     "script": {
#                         "source": """
#                             if (ctx._source.brand_category == null) {
#                                 ctx._source.brand_category = params.new_entries;
#                             } else {
#                                 for (entry in params.new_entries) {
#                                     if (!ctx._source.brand_category.contains(entry)) {
#                                         ctx._source.brand_category.add(entry);
#                                     }
#                                 }
#                             }

#                             if (ctx._source.brand_category_product_type == null) {
#                                 ctx._source.brand_category_product_type = params.new_entries_product_type;
#                             } else {
#                                 for (entry in params.new_entries_product_type) {
#                                     if (!ctx._source.brand_category_product_type.contains(entry)) {
#                                         ctx._source.brand_category_product_type.add(entry);
#                                     }
#                                 }
#                             }

#                             if (ctx._source.brand_category_product_type_attribute == null) {
#                                 ctx._source.brand_category_product_type_attribute = params.new_entries_attribute;
#                             } else {
#                                 for (entry in params.new_entries_attribute) {
#                                     if (!ctx._source.brand_category_product_type_attribute.contains(entry)) {
#                                         ctx._source.brand_category_product_type_attribute.add(entry);
#                                     }
#                                 }
#                             }
#                         """,
#                         "params": {
#                             "new_entries": [new_entry],
#                             "new_entries_product_type": [product_type_entry],
#                             "new_entries_attribute": attribute_entries,
#                         },
#                     },
#                     "upsert": {
#                         "brand_name": brand.brand_name,
#                         "brand_category": [new_entry],
#                         "brand_category_product_type": [product_type_entry],
#                         "brand_category_product_type_attribute": attribute_entries,
#                     },
#                 }
#             )

#             # -------- Product Doc --------
#             product_actions.append(
#                 {
#                     "_op_type": "index",
#                     "_id": product.id,
#                     "brand": brand.brand_name,
#                     "suggest": list(
#                         suggest_set
#                     ),  # unique suggestions including attributes
#                     **data,
#                 }
#             )

#         # Bulk insert
#         auto_success, auto_errors = autosuggest_service.bulk(autosuggest_actions)
#         prod_success, prod_errors = product_service.bulk(product_actions)

#         print(f"✅ Autosuggest indexed: {auto_success}")
#         print(f"✅ Product indexed: {prod_success}")

#         if auto_errors:
#             print(f"❌ Autosuggest errors: {len(auto_errors)}")

#         if prod_errors:
#             print(f"❌ Product errors: {len(prod_errors)}")

#         total_processed += len(products)
#         offset += batch_size
#         batch_number += 1

#         print(f"📊 Total processed so far: {total_processed}")

#     print(f"\n🎉 Sync completed! Total records processed: {total_processed}")

#     return {"total_processed": total_processed}


# async def get_product_list_v6(
#     es: Elasticsearch,
#     query: str = None,
#     brand: Optional[List[str]] = None,
#     product_type: Optional[List[str]] = None,
#     category: Optional[List[str]] = None,
#     min_price: Optional[float] = None,
#     max_price: Optional[float] = None,
#     sort_by: str = "relevance",  # relevance | product_name | base_price
#     sort_order: str = "desc",
#     page: int = 1,
#     size: int = 50,
#     index: str = ESCollection.PRODUCT_V7.value,
# ) -> Dict[str, Any]:

#     # -----------------------------
#     # Build filters
#     # -----------------------------
#     filters = []
#     if brand:
#         filters.append({"terms": {"brand.keyword": brand}})
#     if product_type:
#         filters.append({"terms": {"product_type.keyword": product_type}})
#     if category:
#         filters.append({"terms": {"category.keyword": category}})
#     if min_price is not None or max_price is not None:
#         price_range = {}
#         if min_price is not None:
#             price_range["gte"] = min_price
#         if max_price is not None:
#             price_range["lte"] = max_price
#         filters.append({"range": {"base_price": price_range}})

#     # -----------------------------
#     # Build query
#     # -----------------------------
#     if query:
#         query_body = {
#             "bool": {
#                 "should": [
#                     {"term": {"suggest.keyword": query}},  # exact match
#                     {"match_phrase_prefix": {"suggest": query}},  # prefix match
#                 ],
#                 "minimum_should_match": 1,
#             }
#         }
#     else:
#         query_body = {"match_all": {}}

#     # Total documents in the index (without filters)
#     total_docs_resp = es.count(index=index)
#     total_docs = total_docs_resp.get("count", 0)

#     # Total after applying query + filters
#     total_hits_resp = es.count(
#         index=index,
#         body={"query": {"bool": {"must": [query_body], "filter": filters}}},
#     )
#     total_hits = total_hits_resp.get("count", 0)

#     # -----------------------------
#     # Sorting
#     # -----------------------------
#     es_sort = []
#     if sort_by == "product_name":
#         es_sort.append(
#             {"product_name.keyword": {"order": sort_order, "missing": "_last"}}
#         )
#     elif sort_by == "base_price":
#         es_sort.append({"base_price": {"order": sort_order, "missing": "_last"}})
#     else:
#         es_sort.append({"_score": {"order": sort_order}})

#     # -----------------------------
#     # Final search body
#     # -----------------------------
#     body = {
#         "from": (page - 1) * size,
#         "size": size,
#         "_source": [
#             "product_name",
#             "brand",
#             "category",
#             "base_price",
#             "images.url",
#             "product_type",
#             "suggest",
#         ],
#         "query": {"bool": {"must": [query_body]}},
#         "post_filter": {"bool": {"must": filters}},
#         "sort": es_sort,
#         "aggs": {
#             "brands": {
#                 "filter": {
#                     "bool": {
#                         "must": [
#                             f
#                             for f in filters
#                             if not f.get("terms", {}).get("brand.keyword")
#                         ]
#                     }
#                 },
#                 "aggs": {
#                     "values": {
#                         "terms": {
#                             "field": "brand.keyword",
#                             "size": 1000,
#                             "order": {"_key": "asc"},
#                         }
#                     }
#                 },
#             },
#             "product_type": {
#                 "filter": {
#                     "bool": {
#                         "must": [
#                             f
#                             for f in filters
#                             if not f.get("terms", {}).get("product_type.keyword")
#                         ]
#                     }
#                 },
#                 "aggs": {
#                     "values": {
#                         "terms": {
#                             "field": "product_type.keyword",
#                             "size": 1000,
#                             "order": {"_key": "asc"},
#                         }
#                     }
#                 },
#             },
#             "categories": {
#                 "filter": {
#                     "bool": {
#                         "must": [
#                             f
#                             for f in filters
#                             if not f.get("terms", {}).get("category.keyword")
#                         ]
#                     }
#                 },
#                 "aggs": {
#                     "values": {
#                         "terms": {
#                             "field": "category.keyword",
#                             "size": 1000,
#                             "order": {"_key": "asc"},
#                         }
#                     }
#                 },
#             },
#         },
#     }

#     # -----------------------------
#     # Execute search
#     # -----------------------------
#     resp = es.search(index=index, body=body)
#     hits = resp.get("hits", {}).get("hits", [])

#     # -----------------------------
#     # Extract facets
#     # -----------------------------
#     brand_list = [
#         bucket["key"]
#         for bucket in resp.get("aggregations", {})
#         .get("brands", {})
#         .get("values", {})
#         .get("buckets", [])
#     ]
#     product_type_list = [
#         bucket["key"]
#         for bucket in resp.get("aggregations", {})
#         .get("product_type", {})
#         .get("values", {})
#         .get("buckets", [])
#     ]
#     category_list = [
#         bucket["key"]
#         for bucket in resp.get("aggregations", {})
#         .get("categories", {})
#         .get("values", {})
#         .get("buckets", [])
#     ]

#     # -----------------------------
#     # Format results with attributes included in suggest
#     # -----------------------------
#     results = []
#     for hit in hits:
#         source = hit["_source"]
#         suggest_set = set(source.get("suggest", []))

#         # Add brand + category + product_type + attributes
#         attributes = source.get("attributes", [])
#         brand_name = source.get("brand", "")
#         category_name = source.get("category", "")
#         product_type_name = source.get("product_type", "")

#         for attr in attributes:
#             attr_name = attr.get("name")
#             attr_value = attr.get("value")
#             if attr_name and attr_value:
#                 attr_entry = f"{brand_name} {category_name} {product_type_name} {attr_name} {attr_value}"
#                 suggest_set.add(attr_entry)

#         results.append(
#             {
#                 "id": hit["_id"],
#                 "score": hit["_score"],
#                 "name": source.get("product_name"),
#                 "brand": brand_name,
#                 "product_type": product_type_name,
#                 "category": category_name,
#                 "base_price": source.get("base_price"),
#                 "images": [
#                     i.get("url")
#                     for i in source.get("images", [])
#                     if isinstance(i, dict)
#                 ],
#                 "suggest": list(suggest_set),  # updated suggestions
#             }
#         )

#     return {
#         "total": total_hits,
#         "total_docs_after_filter": total_hits,
#         "total_docs": total_docs,
#         "page": page,
#         "size": size,
#         "total_pages": (total_hits + size - 1) // size,
#         "results": results,
#         "facets": {
#             "brands": brand_list,
#             "categories": category_list,
#             "product_type": product_type_list,
#         },
#     }


def add_sort(es_sort, field, sort_order):
    # 1. Primary Sort: Push null/empty values to the bottom
    # We return 0 for valid data and 1 for empty data.
    # When sorting ASC, 0 (valid) comes first. When DESC, we still want valid first,
    # so we logic this specifically.
    es_sort.append(
        {
            "_script": {
                "type": "number",
                "order": "asc",  # Always 'asc' so 0 (valid) comes before 1 (empty)
                "script": {
                    "lang": "painless",
                    "source": """
                    if (!doc.containsKey(params.f) || doc[params.f].empty || doc[params.f].value == null || doc[params.f].value == '') {
                        return 1;
                    }
                    return 0;
                """,
                    "params": {"f": f"{field}.keyword"},
                },
            }
        }
    )

    # 2. Secondary Sort: Case-insensitive alphabetical sort
    es_sort.append(
        {
            "_script": {
                "type": "string",
                "order": sort_order,
                "script": {
                    "lang": "painless",
                    "source": """
                    if (!doc.containsKey(params.f) || doc[params.f].empty) {
                        return "";
                    }
                    return doc[params.f].value.toLowerCase();
                """,
                    "params": {"f": f"{field}.keyword"},
                },
            }
        }
    )


def add_sort(es_sort, field, sort_order):
    es_sort.append(
        {
            "_script": {
                "type": "number",
                "order": "asc",
                "script": {
                    "lang": "painless",
                    "source": """
                    if (!doc.containsKey(params.f) || doc[params.f].empty || doc[params.f].value == null || doc[params.f].value == '') {
                        return 1;
                    }
                    return 0;
                """,
                    "params": {"f": f"{field}.keyword"},
                },
            }
        }
    )

    es_sort.append(
        {
            "_script": {
                "type": "string",
                "order": sort_order,
                "script": {
                    "lang": "painless",
                    "source": """
                    if (!doc.containsKey(params.f) || doc[params.f].empty) {
                        return "";
                    }
                    return doc[params.f].value.toLowerCase();
                """,
                    "params": {"f": f"{field}.keyword"},
                },
            }
        }
    )


async def get_product_list_v6(
    es: Elasticsearch,
    query: str = None,
    brand: Optional[List[str]] = None,
    product_type: Optional[List[str]] = None,
    category: Optional[List[str]] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    sort_by: str = "relevance",
    sort_order: str = "desc",
    page: int = 1,
    size: int = 50,
    index: str = ESCollection.PRODUCT_V7.value,
) -> Dict[str, Any]:

    # -----------------------------
    # 🔥 SYNONYMS
    # -----------------------------
    SYNONYMS = {
        "mobile": ["smartphone", "cellphone"],
        "tv": ["television"],
        "laptop": ["notebook"],
        "shoe": ["shoes"],
        "headphone": ["headphones"],
        "arm cover": ["sleeve"],
        "ss": ["stainless steel"],
        "marking paints": ["water paints"],
    }

    def get_synonyms(q: str):
        if not q:
            return []
        words = q.lower().split()
        expanded = set()
        for w in words:
            if w in SYNONYMS:
                expanded.update(SYNONYMS[w])
        return list(expanded)

    # -----------------------------
    # Filters
    # -----------------------------
    filters = []
    if brand:
        filters.append({"terms": {"brand.keyword": brand}})
    if product_type:
        filters.append({"terms": {"product_type.keyword": product_type}})
    if category:
        filters.append({"terms": {"category.keyword": category}})
    if min_price is not None or max_price is not None:
        price_range = {}
        if min_price is not None:
            price_range["gte"] = min_price
        if max_price is not None:
            price_range["lte"] = max_price
        filters.append({"range": {"base_price": price_range}})

    print("🔹 Filters applied:", filters)

    # -----------------------------
    # Base Query (UNCHANGED)
    # -----------------------------
    def build_query(q):
        return {
            "bool": {
                "should": [
                    {"term": {"suggest.keyword": {"value": q, "boost": 20}}},
                    {"match_phrase": {"product_name": {"query": q, "boost": 12}}},
                    {"match_phrase": {"suggest": {"query": q, "boost": 10}}},
                    {"match_phrase_prefix": {"suggest": {"query": q, "boost": 6}}},
                    {"match": {"brand": {"query": q, "operator": "and", "boost": 5}}},
                    {
                        "multi_match": {
                            "query": q,
                            "fields": [
                                "product_name^3",
                                "brand^4",
                                "suggest^2",
                                "search_keywords",
                            ],
                            "fuzziness": "AUTO",
                            "operator": "and",
                            "boost": 2,
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }

    # -----------------------------
    # Query with synonyms
    # -----------------------------
    if query:
        main_query = build_query(query)
        synonym_terms = get_synonyms(query)
        synonym_queries = [build_query(s) for s in synonym_terms]

        query_body = {
            "bool": {
                "should": [main_query, *synonym_queries],
                "minimum_should_match": 1,
            }
        }
    else:
        query_body = {"match_all": {}}

    print("🔹 Query body:", query_body)

    # -----------------------------
    # Counts
    # -----------------------------
    total_docs_resp = es.count(index=index)
    total_docs = total_docs_resp.get("count", 0)

    total_hits_resp = es.count(
        index=index,
        body={"query": {"bool": {"must": [query_body], "filter": filters}}},
    )
    total_hits = total_hits_resp.get("count", 0)

    # -----------------------------
    # Sorting
    # -----------------------------
    es_sort = []

    if sort_by == "product_name":
        es_sort.append(
            {"product_name.keyword": {"order": sort_order, "missing": "_last"}}
        )
    elif sort_by == "base_price":
        es_sort.append({"base_price": {"order": sort_order, "missing": "_last"}})
    elif sort_by in ["brand", "category", "product_type"]:
        add_sort(es_sort, sort_by, sort_order)
    elif sort_by == "view_popularity":
        es_sort.append({"view_count": {"order": sort_order, "missing": "_last"}})
    elif sort_by == "search_popularity":
        es_sort.append({"search_popularity": {"order": sort_order, "missing": "_last"}})
    else:
        es_sort.append({"_score": {"order": sort_order}})

    MAX_RESULT_WINDOW = 10000
    from_ = (page - 1) * size

    if from_ >= MAX_RESULT_WINDOW:
        import random

        from_ = random.randint(0, MAX_RESULT_WINDOW - size)

    # -----------------------------
    # FINAL SEARCH BODY (FACETS ADDED)
    # -----------------------------
    body = {
        "from": from_,
        "size": size,
        "_source": [
            "product_name",
            "brand",
            "category",
            "base_price",
            "images.url",
            "product_type",
            "suggest",
            "view_count",
            "search_popularity",
        ],
        "query": query_body,
        "post_filter": {"bool": {"must": filters}},
        "sort": es_sort,
        # ✅ FACETS
        "aggs": {
            "brands": {
                "filter": {
                    "bool": {
                        "must": [
                            f
                            for f in filters
                            if not f.get("terms", {}).get("brand.keyword")
                        ]
                    }
                },
                "aggs": {
                    "values": {
                        "terms": {
                            "field": "brand.keyword",
                            "size": 1000,
                            "order": {"_key": "asc"},
                        }
                    }
                },
            },
            "product_type": {
                "filter": {
                    "bool": {
                        "must": [
                            f
                            for f in filters
                            if not f.get("terms", {}).get("product_type.keyword")
                        ]
                    }
                },
                "aggs": {
                    "values": {
                        "terms": {
                            "field": "product_type.keyword",
                            "size": 1000,
                            "order": {"_key": "asc"},
                        }
                    }
                },
            },
            "categories": {
                "filter": {
                    "bool": {
                        "must": [
                            f
                            for f in filters
                            if not f.get("terms", {}).get("category.keyword")
                        ]
                    }
                },
                "aggs": {
                    "values": {
                        "terms": {
                            "field": "category.keyword",
                            "size": 1000,
                            "order": {"_key": "asc"},
                        }
                    }
                },
            },
        },
    }

    print("🔹 Final search body prepared")

    resp = es.search(index=index, body=body)
    hits = resp.get("hits", {}).get("hits", [])

    print(f"🔹 Number of hits: {len(hits)}")

    # -----------------------------
    # Extract facets
    # -----------------------------
    brand_list = [
        b["key"]
        for b in resp.get("aggregations", {})
        .get("brands", {})
        .get("values", {})
        .get("buckets", [])
    ]

    product_type_list = [
        b["key"]
        for b in resp.get("aggregations", {})
        .get("product_type", {})
        .get("values", {})
        .get("buckets", [])
    ]

    category_list = [
        b["key"]
        for b in resp.get("aggregations", {})
        .get("categories", {})
        .get("values", {})
        .get("buckets", [])
    ]

    # -----------------------------
    # Results
    # -----------------------------
    results = []
    for hit in hits:
        source = hit["_source"]

        results.append(
            {
                "id": hit["_id"],
                "score": hit["_score"],
                "name": source.get("product_name"),
                "brand": source.get("brand"),
                "product_type": source.get("product_type"),
                "category": source.get("category"),
                "view_count": source.get("view_count", 0),
                "search_popularity": source.get("search_popularity", 0),
                "base_price": source.get("base_price"),
                "images": [
                    i.get("url")
                    for i in source.get("images", [])
                    if isinstance(i, dict)
                ],
                "suggest": source.get("suggest", []),
            }
        )

    return {
        "total": total_hits,
        "total_docs_after_filter": total_hits,
        "total_docs": total_docs,
        "page": page,
        "size": size,
        "total_pages": (total_hits + size - 1) // size,
        "results": results,
        # ✅ FACETS RETURNED
        "facets": {
            "brands": brand_list,
            "categories": category_list,
            "product_type": product_type_list,
        },
    }


from elasticsearch.helpers import streaming_bulk


async def sync_product_suggest_data_es_v6(
    es: Elasticsearch,
    session: AsyncSession,
    batch_size: int = 30,
    start_id: int = 1,
) -> dict:

    autosuggest_index = ESCollection.PRODUCT_AUTO_SUGGEST_V7.value
    product_index = ESCollection.PRODUCT_V7.value

    # Ensure indexes exist
    await create_or_get_index_v6(es, autosuggest_index, autosuggest_index)
    await create_or_get_index_v6(es, product_index, product_index)

    offset = 0
    total_processed = 0
    batch_number = 1

    print("🚀 Starting sync...")

    # ======================
    # Bulk helper with retry
    # ======================
    def log_bulk(actions, index_name):
        created = updated = failed = noop = 0
        if not actions:
            return created, updated, noop, failed

        failed_actions = []

        def generate():
            for action in actions:
                new_action = action.copy()
                new_action["_index"] = index_name
                yield new_action

        for ok, item in streaming_bulk(es, generate()):
            if not ok:
                print("❌ Bulk item failed:", item)
                failed += 1
                failed_actions.append(item)
                continue

            op_type = list(item.keys())[0]
            result = item[op_type].get("result")
            if result == "created":
                created += 1
            elif result == "updated":
                updated += 1
            elif result == "noop":
                noop += 1

        # Retry failed actions once
        if failed_actions:
            print(f"🔄 Retrying {len(failed_actions)} failed actions...")
            for ok, item in streaming_bulk(es, (i.copy() for i in failed_actions)):
                if not ok:
                    print("❌ Retry failed:", item)
                    failed += 1
                    continue

                op_type = list(item.keys())[0]
                result = item[op_type].get("result")
                if result == "created":
                    created += 1
                elif result == "updated":
                    updated += 1
                elif result == "noop":
                    noop += 1

        return created, updated, noop, failed

    # ======================
    # Main loop
    # ======================
    while True:
        print(f"\n📦 Fetching batch {batch_number} (offset={offset})")

        result = await session.execute(
            select(Product)
            .options(
                selectinload(Product.brand),
                selectinload(Product.category),
                selectinload(Product.images),
                selectinload(Product.features),
                selectinload(Product.attributes),
                selectinload(Product.videos),
                selectinload(Product.documents),
                selectinload(Product.industry),
                selectinload(Product.product_type),
            )
            .where(Product.id >= start_id)
            .order_by(Product.id)
            .offset(offset)
            .limit(batch_size)
        )

        products = result.scalars().all()
        if not products:
            print("✅ No more data to process.")
            break

        autosuggest_actions = []
        product_actions = []

        for product in products:
            brand = product.brand
            if not brand:
                continue

            category = product.category
            product_type_obj = product.product_type
            attributes = product.attributes or []
            features = product.features or []
            images = product.images or []
            videos = product.videos or []
            documents = product.documents or []

            # --------------------
            # Product payload
            # --------------------
            data = {
                "product_name": product.product_name or "",
                "sku": product.sku or "",
                "mpn": product.mpn or "",
                "gtin": product.gtin or "",
                "ean": product.ean or "",
                "upc": product.upc or "",
                "product_type": (
                    product_type_obj.product_type if product_type_obj else ""
                ),
                "industry_name": (
                    product.industry.industry_name if product.industry else ""
                ),
                "category_name": category.name if category else "",
                "taxonomy": product.taxonomy or "",
                "country_of_origin": product.country_of_origin or "",
                "warranty": product.warranty or "",
                "weight": product.weight or 0,
                "length": product.length or 0,
                "width": product.width or 0,
                "height": product.height or 0,
                "base_price": product.base_price or 0,
                "sale_price": product.sale_price or 0,
                "selling_price": product.selling_price or 0,
                "special_price": product.special_price or 0,
                "weight_unit": product.weight_unit or "",
                "dimension_unit": product.dimension_unit or "",
                "currency": product.currency or "",
                "stock_status": product.stock_status or "",
                "vendor_name": product.vendor_name or "",
                "vendor_sku": product.vendor_sku or "",
                "short_description": product.short_description or "",
                "long_description": product.long_description or "",
                "meta_title": product.meta_title or "",
                "meta_description": product.meta_description or "",
                "search_keywords": product.search_keywords or "",
                "stock_qty": product.stock_qty or 0,
                "features": [
                    {"name": f.name or "", "value": f.value or ""} for f in features
                ],
                "attributes": [
                    {
                        "name": a.attribute_name or "",
                        "value": a.attribute_value or "",
                        "uom": a.attribute_uom or "",
                    }
                    for a in attributes
                ],
                "images": [{"name": i.name or "", "url": i.url or ""} for i in images],
                "videos": [{"name": v.name or "", "url": v.url or ""} for v in videos],
                "documents": [
                    {"name": d.name or "", "url": d.url or ""} for d in documents
                ],
            }

            # --------------------
            # Suggestions payload
            # --------------------
            brand_name = brand.brand_name or ""
            category_name = category.name if category else ""
            product_type_name = (
                product_type_obj.product_type if product_type_obj else ""
            )

            new_entry = f"{brand_name} {category_name}".strip()
            product_type_entry = (
                f"{brand_name} {category_name} {product_type_name}".strip()
            )
            suggest_set = {
                brand_name,
                new_entry,
                product_type_entry,
                product.product_name or "",
                product.mpn or "",
                product.sku or "",
            }

            for attr in attributes:
                full_entry = f"{brand_name} {category_name} {product_type_name} {attr.attribute_name or ''} {attr.attribute_value or ''}".strip()
                suggest_set.add(full_entry)

            # --------------------
            # Autosuggest document (must create)
            # --------------------
            autosuggest_actions.append(
                {
                    "_op_type": "update",
                    "_id": brand.id,
                    "script": {
                        "source": """
                    if (ctx._source.brand_category == null) {
                        ctx._source.brand_category = params.new_entries;
                    } else if (!(ctx._source.brand_category instanceof List)) {
                        ctx._source.brand_category = [];
                    }
                    for (entry in params.new_entries) {
                        if (!ctx._source.brand_category.contains(entry)) {
                            ctx._source.brand_category.add(entry);
                        }
                    }
                    """,
                        "params": {"new_entries": [new_entry]},
                    },
                    "upsert": {
                        "brand_name": brand_name,
                        "brand_category": [new_entry],
                    },
                }
            )

            # --------------------
            # Product document
            # --------------------
            product_actions.append(
                {
                    "_op_type": "index",
                    "_id": product.id,
                    "brand": brand_name,
                    "suggest": list(suggest_set),
                    **data,
                }
            )

        # --------------------
        # Execute bulk safely
        # --------------------
        print(
            f"Processing batch {batch_number} → Autosuggest: {len(autosuggest_actions)}, Products: {len(product_actions)}"
        )
        a_created, a_updated, a_noop, a_failed = log_bulk(
            autosuggest_actions, autosuggest_index
        )
        p_created, p_updated, p_noop, p_failed = log_bulk(
            product_actions, product_index
        )

        print(
            f"Batch {batch_number} result → Autosuggest: Created {a_created}, Updated {a_updated}, Failed {a_failed}"
        )
        print(
            f"Batch {batch_number} result → Products: Created {p_created}, Updated {p_updated}, Failed {p_failed}"
        )

        total_processed += len(products)
        offset += batch_size
        batch_number += 1

    print(f"\n🎉 Sync completed! Total processed: {total_processed}")
    return {"total_processed": total_processed}
