import re
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from sqlalchemy.orm import selectinload
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from app.models import Product
from app.services import ESCollection, ElasticsearchIndexManager


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


def add_sort(es_sort, field, sort_order):
    field_key = f"{field}.keyword"

    # STEP 1: The "Null/Empty" Priority Sort
    # Remains "asc" so 0 (valid) comes before 1 (null/empty)
    es_sort.append(
        {
            "_script": {
                "type": "number",
                "order": "asc",
                "script": {
                    "lang": "painless",
                    "source": """
                    if (!doc.containsKey(params.f) || doc[params.f].size() == 0 || doc[params.f].value == null || doc[params.f].value == "") {
                        return 1;
                    }
                    return 0;
                """,
                    "params": {"f": field_key},
                },
            }
        }
    )

    # STEP 2: The Case-Insensitive Data Sort
    # We use a script to lowercase the value so 'Apple' and 'apple' are equal
    es_sort.append(
        {
            "_script": {
                "type": "string",
                "order": sort_order,
                "script": {
                    "lang": "painless",
                    "source": """
                    if (doc[params.f].size() == 0) return "";
                    return doc[params.f].value.toLowerCase();
                """,
                    "params": {"f": field_key},
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
    attr_filters: Optional[Dict[str, List[str]]] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    sort_by: str = "relevance",
    sort_order: str = "desc",
    page: int = 1,
    size: int = 50,
    index: str = "product_v7",
    end_date: datetime = None,
) -> Dict[str, Any]:

    start_total = time.perf_counter()

    ES_MAX_WINDOW = 10000
    requested_from = (page - 1) * size

    if (requested_from + size) > ES_MAX_WINDOW:
        current_from = ES_MAX_WINDOW - size
    else:
        current_from = requested_from

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

    runtime_mappings = {
        "attr_pairs": {
            "type": "keyword",
            "script": {
                "source": """
                if (params['_source'].attributes != null) {
                    for (item in params['_source'].attributes) {
                        if (item.name != null && item.value != null && item.value != "") {
                            emit(item.name + ":::" + item.value);
                        }
                    }
                }
                """
            },
        }
    }

    # 1. Build Filters & Query
    start_build = time.perf_counter()
    filters = []

    if end_date:
        filters.append(
            {
                "bool": {
                    "should": [
                        {"range": {"created_at": {"lte": end_date.isoformat()}}},
                        {"bool": {"must_not": {"exists": {"field": "created_at"}}}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )
    if brand:
        filters.append({"terms": {"brand.keyword": brand}})
    if product_type:
        filters.append({"terms": {"product_type.keyword": product_type}})
    if category:
        filters.append({"terms": {"category.keyword": category}})

    if attr_filters:
        for attr_name, values in attr_filters.items():
            combined_pairs = [f"{attr_name}:::{v}" for v in values]
            filters.append({"terms": {"attr_pairs": combined_pairs}})

    if min_price is not None or max_price is not None:
        price_range = {}
        if min_price is not None:
            price_range["gte"] = min_price
        if max_price is not None:
            price_range["lte"] = max_price
        filters.append({"range": {"base_price": price_range}})

    def build_query(q):
        if not q:
            return {"match_all": {}}

        ignore_list = ["3m"]

        # Pre-processing: q_clean for exact matches, q_expanded for broad/OR matches
        q_clean = q.strip()
        if q.lower() not in ignore_list:
            q_expanded = re.sub(r"(\d+)([a-zA-Z]+)", r"\1 \2", q_clean.lower())
        else:
            q_expanded = q

        return {
            "function_score": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": q_expanded,
                                    "fields": [
                                        "brand^50",
                                        "product_type^30",
                                        "category^20",
                                        "suggest^20",
                                    ],
                                    "operator": "or",
                                    "minimum_should_match": 1,
                                }
                            }
                        ],
                        "should": [
                            # --- 1. PRIORITY EXACT MATCHES (Using q_clean) ---
                            {
                                "term": {
                                    "sku.keyword": {
                                        "value": q_clean,
                                        "boost": 200000,
                                        "case_insensitive": True,
                                    }
                                }
                            },
                            {
                                "term": {
                                    "mpn.keyword": {
                                        "value": q_clean,
                                        "boost": 180000,
                                        "case_insensitive": True,
                                    }
                                }
                            },
                            {
                                "term": {
                                    "product_name.keyword": {
                                        "value": q_clean,
                                        "boost": 150000,
                                        "case_insensitive": True,
                                    }
                                }
                            },
                            # --- 2. EXISTING LOGIC (Analyzed matches) ---
                            {
                                "term": {
                                    "brand": {
                                        "value": q_clean,
                                        "boost": 30000,
                                        "case_insensitive": True,
                                    }
                                }
                            },
                            {
                                "term": {
                                    "brand.keyword": {
                                        "value": q_clean,
                                        "boost": 30000,
                                        "case_insensitive": True,
                                    }
                                }
                            },
                            {"match": {"sku": {"query": q_clean, "boost": 5000}}},
                            {"match": {"mpn": {"query": q_clean, "boost": 4000}}},
                            {
                                "term": {
                                    "brand.keyword": {
                                        "value": q_expanded,
                                        "boost": 10000,
                                    }
                                }
                            },
                            {
                                "match_phrase": {
                                    "brand": {"query": q_expanded, "boost": 5000}
                                }
                            },
                            {
                                "term": {
                                    "suggest.keyword": {
                                        "value": q_expanded,
                                        "boost": 8000,
                                    }
                                }
                            },
                            {
                                "match": {
                                    "suggest": {
                                        "query": q_expanded,
                                        "operator": "and",
                                        "boost": 3000,
                                    }
                                }
                            },
                            {
                                "multi_match": {
                                    "query": q_expanded,
                                    "type": "cross_fields",
                                    "fields": [
                                        "brand^100",
                                        "attributes.value^50",
                                        "product_name^50",
                                        "features.value^20",
                                    ],
                                    "operator": "and",
                                    "boost": 2000,
                                }
                            },
                            {
                                "multi_match": {
                                    "query": q_expanded,
                                    "fields": ["attributes.value^80"],
                                    "fuzziness": "AUTO",
                                    "operator": "and",
                                    "boost": 500,
                                }
                            },
                            {
                                "multi_match": {
                                    "query": q_expanded,
                                    "fields": [
                                        "product_name^20",
                                        "brand^60",
                                        "product_type^60",
                                        "category^40",
                                    ],
                                    "fuzziness": "AUTO:4,6",
                                    "prefix_length": 1,
                                    "operator": "and",
                                    "boost": 300,
                                }
                            },
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "boost_mode": "sum",
                "field_value_factor": {
                    "field": "search_popularity",
                    "modifier": "log1p",
                    "missing": 1,
                },
            }
        }

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

    es_sort = []
    if sort_by == "product_name":
        add_sort(es_sort, "product_name", sort_order)
    elif sort_by == "brand":
        add_sort(es_sort, "brand", sort_order)
    elif sort_by == "product_type":
        add_sort(es_sort, "product_type", sort_order)
    elif sort_by == "category":
        add_sort(es_sort, "category", sort_order)
    elif sort_by == "search_popularity":
        es_sort.append(
            {
                "search_popularity": {
                    "order": sort_order,
                    "unmapped_type": "long",
                    "missing": 0,
                }
            }
        )
    elif sort_by == "base_price":
        es_sort.append({"base_price": {"order": sort_order, "missing": "_last"}})
    elif sort_by == "review":
        es_sort.append({"review": {"order": sort_order, "missing": "_last"}})
    elif sort_by == "created_at":
        es_sort.append({"created_at": {"order": sort_order, "missing": "_last"}})
    else:
        es_sort.append({"_score": {"order": sort_order}})

    if sort_by == "created_at":
        cutoff_date = datetime.utcnow() - timedelta(days=2)

        if sort_order == "desc":
            # 🔥 Only last 2 days products
            filters.append({"range": {"created_at": {"gte": cutoff_date.isoformat()}}})

    es_sort.append({"search_popularity": {"order": "desc", "missing": "_last"}})

    body = {
        "runtime_mappings": runtime_mappings,
        "from": current_from,
        "size": size,
        "query": query_body,
        "post_filter": {"bool": {"must": filters}},
        "track_total_hits": True,
        "sort": es_sort,
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
            "review",
            "created_at",
        ],
        "aggs": {
            "all_docs_count": {"global": {}},
            "brands": {
                "filter": {
                    "bool": {
                        "must": [f for f in filters if "brand.keyword" not in str(f)]
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
                            f for f in filters if "product_type.keyword" not in str(f)
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
                        "must": [f for f in filters if "category.keyword" not in str(f)]
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
            "dynamic_attributes_filtered": {
                "filter": {"bool": {"must": filters}},
                "aggs": {"values": {"terms": {"field": "attr_pairs", "size": 2000}}},
            },
        },
    }

    resp = es.search(index=index, body=body)

    first_resp = resp  # IMPORTANT FIX

    total_hits = first_resp.get("hits", {}).get("total", {}).get("value", 0)
    total_docs = (
        first_resp.get("aggregations", {}).get("all_docs_count", {}).get("doc_count", 0)
    )

    hits = first_resp.get("hits", {}).get("hits", [])
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
                "review": source.get("review"),
                "created_at": source.get("created_at"),
                "images": [
                    i.get("url")
                    for i in source.get("images", [])
                    if isinstance(i, dict)
                ],
                "suggest": source.get("suggest", []),
            }
        )

    def get_buckets(agg_name):
        return [
            b["key"]
            for b in first_resp.get("aggregations", {})
            .get(agg_name, {})
            .get("values", {})
            .get("buckets", [])
        ]

    dynamic_facets = {}
    attr_buckets = (
        first_resp.get("aggregations", {})
        .get("dynamic_attributes_filtered", {})
        .get("values", {})
        .get("buckets", [])
    )

    for b in attr_buckets:
        if ":::" in b["key"]:
            name, val = b["key"].split(":::", 1)
            if name not in dynamic_facets:
                dynamic_facets[name] = []
            dynamic_facets[name].append(val)

    total_fn_duration = time.perf_counter() - start_total

    return {
        "total_docs_after_filter": total_hits,
        "total_docs": total_docs,
        "page": page,
        "size": size,
        "total_pages": (total_hits + size - 1) // size if total_hits > 0 else 0,
        "results": results,
        "facets": {
            "brands": get_buckets("brands"),
            "categories": get_buckets("categories"),
            "product_type": get_buckets("product_type"),
            "dynamic_attributes": dynamic_facets,
        },
        # "product_ids": all_ids,
    }


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
