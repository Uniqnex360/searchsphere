from typing import Optional, List, Dict, Any
from elasticsearch import Elasticsearch
from app.services.common import ESCollection


"""
Elasticsearch query to prioritize product name autocomplete:

Steps:
1. First, query the `product_name.suggest` field for prefix matches (autocomplete).
   - If matches exist, these results come first and dominate the score.
2. If no autocomplete matches, fall back to searching:
   - product_name (full text)
   - product_name.ngram (partial match)
   - product_name.keyword (exact match)
3. This is achieved using a `dis_max` query that selects the clause with the highest score.
4. You can adjust `tie_breaker` to balance scores if multiple clauses match.
5. Returns top 10 results.
"""


# -------------------------------
# Field mapping for product name
# -------------------------------
def get_text_field_mapping(
    analyzer: str = "custom_text_analyzer",
    search_analyzer: str = "custom_search_analyzer",
    include_keyword: bool = True,
    include_ngram: bool = True,
    include_suggest: bool = True,
    keyword_ignore_above: int = 256,
) -> dict:
    """
    Returns a reusable Elasticsearch text field mapping with optional subfields:
    - keyword: exact match
    - suggest: completion suggestions (for autocomplete)
    - ngram: partial match autocomplete
    """
    fields = {}

    if include_keyword:
        fields["keyword"] = {"type": "keyword", "ignore_above": keyword_ignore_above}

    if include_suggest:
        fields["suggest"] = {"type": "completion"}

    if include_ngram:
        fields["ngram"] = {
            "type": "text",
            "analyzer": "autocomplete_analyzer",
            "search_analyzer": "standard",
        }

    return {
        "type": "text",
        "analyzer": analyzer,
        "search_analyzer": search_analyzer,
        "fields": fields,
    }


# -------------------------------
# Reusable float field mapping
# -------------------------------
def get_float_field_mapping(
    coerce: bool = True,
    doc_values: bool = True,
    null_value: float | None = None,
) -> dict:
    """
    Returns a reusable Elasticsearch float field mapping with optional settings:
    - coerce: whether to coerce non-float values to float
    - doc_values: enable doc_values for aggregations/sorting
    - null_value: value to use if the field is missing or null
    """
    mapping = {
        "type": "float",
        "coerce": coerce,
        "doc_values": doc_values,
    }

    if null_value is not None:
        mapping["null_value"] = null_value

    return mapping


# -------------------------------
# Field Mappings
# -------------------------------
# text + keyword fields
PRODUCT_NAME_MAPPING = get_text_field_mapping()
BRAND_MAPPING = get_text_field_mapping()
CATEGORY_NAME_MAPPING = get_text_field_mapping()
INDUSTRY_NAME_MAPPING = get_text_field_mapping()
SKU_MAPPING = get_text_field_mapping(include_suggest=False)
MPN_MAPPING = get_text_field_mapping(include_suggest=False)
TAXONOMY_MAPPING = get_text_field_mapping(include_suggest=False)
VENDOR_NAME_MAPPING = get_text_field_mapping(include_suggest=False)


# float fields
BASE_PRICE_MAPPING = get_float_field_mapping()

# text fields
SHORT_DESCRIPTION = get_text_field_mapping(include_keyword=False, include_suggest=False)
LONG_DESCRIPTION = get_text_field_mapping(include_keyword=False, include_suggest=False)

# nested fields
FEATURES_MAPPING = {
    "type": "nested",
    "properties": {"value": get_text_field_mapping(include_keyword=False)},
}
ATTRIBUTE_MAPPING = {
    "type": "nested",
    "properties": {
        "name": get_text_field_mapping(),
        "value": get_text_field_mapping(),
        "uom": get_text_field_mapping(),
    },
}


# -------------------------------
# Index settings including analyzers
# -------------------------------
SETTINGS_MAPPING = {
    "analysis": {
        "tokenizer": {
            "edge_ngram_tokenizer": {
                "type": "edge_ngram",
                "min_gram": 2,
                "max_gram": 20,
                "token_chars": ["letter", "digit"],
            }
        },
        "filter": {
            "english_stop": {"type": "stop", "stopwords": "_english_"},
            "word_delimiter_graph_filter": {
                "type": "word_delimiter_graph",
                "preserve_original": True,
                "split_on_case_change": True,
                "split_on_numerics": True,
                "catenate_all": False,
                "catenate_numbers": False,
                "generate_number_parts": True,
            },
        },
        "analyzer": {
            "custom_text_analyzer": {
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "asciifolding",
                    "word_delimiter_graph_filter",
                    "english_stop",
                ],
            },
            "custom_search_analyzer": {
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "asciifolding",
                    "word_delimiter_graph_filter",
                    "english_stop",
                ],
            },
            "autocomplete_analyzer": {
                "tokenizer": "edge_ngram_tokenizer",
                "filter": ["lowercase", "asciifolding"],
            },
        },
    }
}

# -------------------------------
# Full index mapping
# -------------------------------
PRODUCT_V2_MAPPING = {
    "mappings": {
        "properties": {
            "product_name": PRODUCT_NAME_MAPPING,
            "brand": BRAND_MAPPING,
            "category": CATEGORY_NAME_MAPPING,
            "sku": SKU_MAPPING,
            "industry_name": INDUSTRY_NAME_MAPPING,
            "mpn": MPN_MAPPING,
            "taxonomy": TAXONOMY_MAPPING,
            "vendor_name": VENDOR_NAME_MAPPING,
            "base_price": BASE_PRICE_MAPPING,
            "short_description": SHORT_DESCRIPTION,
            "long_description": LONG_DESCRIPTION,
            "features": FEATURES_MAPPING,
            "attributes": ATTRIBUTE_MAPPING,
        }
    },
    "settings": SETTINGS_MAPPING,
}


# -------------------------------
# function to create index
# -------------------------------
def create_product_mapping(es: Elasticsearch):
    """
    Creates the product index with the specified mapping and settings.
    """
    index_name = ESCollection.PRODUCT_V2.value

    # Check if index exists
    if not es.indices.exists(index=index_name):
        # Create index with mapping and settings
        es.indices.create(index=index_name, body=PRODUCT_V2_MAPPING)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")
        # Index exists, try updating the mapping
        try:
            es.indices.put_mapping(
                index=index_name, body=PRODUCT_V2_MAPPING["mappings"]
            )
            print(f"Index '{index_name}' mapping updated successfully.")
        except Exception as e:
            print(f"Failed to update mapping: {e}")


# -----------------------------
# MULTI-SELECT FILTER ENGINE
# -----------------------------
def apply_filters(filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    must = []
    if not filters:
        return must

    if filters.get("brand"):
        must.append({"terms": {"brand.keyword": filters["brand"]}})
    if filters.get("category"):
        must.append({"terms": {"category.keyword": filters["category"]}})
    price_range = {}
    if filters.get("price_min") is not None:
        price_range["gte"] = filters["price_min"]
    if filters.get("price_max") is not None:
        price_range["lte"] = filters["price_max"]
    if price_range:
        must.append({"range": {"base_price": price_range}})
    return must


async def get_product_auto_complete_v4(
    es: Elasticsearch,
    query: str = None,
    brand: Optional[List[str]] = None,
    product_type: Optional[List[str]] = None,
    category: Optional[List[str]] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    sort_by: str = "relevance",  # relevance | product_name | base_price
    sort_order: str = "desc",
    page: int = 1,
    size: int = 50,
    index: str = ESCollection.PRODUCT_V2.value,
) -> Dict[str, Any]:

    # -----------------------------
    # Build filters
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

    # -----------------------------
    # Build query
    # -----------------------------
    if query:
        query_body = {
            "dis_max": {
                "tie_breaker": 0.3,
                "queries": [
                    # Partial match ngram
                    {"match": {"product_name.ngram": {"query": query}}},
                    # Full text match
                    {"match": {"product_name": {"query": query}}},
                    # Exact match
                    {"term": {"product_name.keyword": query.lower()}},
                    # Brand match
                    {"match": {"brand": {"query": query}}},
                    # Product type match
                    {"match": {"product_type": {"query": query}}},
                    # Category match
                    {"match": {"category": {"query": query}}},
                    # SKU match
                    {"match": {"sku": {"query": query}}},
                    # MPN match
                    {"match": {"mpn": {"query": query}}},
                    # Nested attributes match (name or value)
                    {
                        "nested": {
                            "path": "attributes",
                            "query": {
                                "bool": {
                                    "should": [
                                        {
                                            "match": {
                                                "attributes.name": {"query": query}
                                            }
                                        },
                                        {
                                            "match": {
                                                "attributes.value": {"query": query}
                                            }
                                        },
                                        {"match": {"attributes.uom": {"query": query}}},
                                    ],
                                    "minimum_should_match": 1,
                                }
                            },
                        }
                    },
                ],
            }
        }
    else:
        query_body = {"match_all": {}}

    # Total number of documents in the index (ignoring filters)
    total_docs_resp = es.count(index=index)
    total_docs = total_docs_resp.get("count", 0)

    # Total after applying search + filters
    total_hits_resp = es.count(
        index=index,
        body={"query": {"bool": {"must": [query_body], "filter": filters}}},
    )
    total_hits = total_hits_resp.get("count", 0)

    # -----------------------------
    # Sorting (nulls last)
    # -----------------------------
    es_sort = []
    if sort_by == "product_name":
        es_sort.append(
            {"product_name.keyword": {"order": sort_order, "missing": "_last"}}
        )
    elif sort_by == "base_price":
        es_sort.append({"base_price": {"order": sort_order, "missing": "_last"}})
    else:  # relevance
        es_sort.append({"_score": {"order": sort_order}})

    # -----------------------------
    # Suggest (top-level)
    # -----------------------------
    suggest_body = {
        "product_suggest": {
            "prefix": query or "",
            "completion": {
                "field": "product_name.suggest",
                "fuzzy": {"fuzziness": "AUTO"},
                "size": 10,
            },
        }
    }

    # -----------------------------
    # Final search body
    # -----------------------------
    body = {
        "from": (page - 1) * size,
        "size": size,
        "_source": ["product_name", "brand", "category", "base_price", "images.url"],
        # "query": {"bool": {"must": [query_body], "filter": filters}},
        "query": {"bool": {"must": [query_body]}},
        "post_filter": {"bool": {"must": filters}},
        "sort": es_sort,
        "suggest": suggest_body,
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

    # -----------------------------
    # Execute search
    # -----------------------------
    resp = es.search(index=index, body=body)
    hits = resp.get("hits", {}).get("hits", [])

    brand_list = [
        bucket["key"]
        for bucket in resp.get("aggregations", {})
        .get("brands", {})
        .get("values", {})
        .get("buckets", [])
    ]

    product_type_list = [
        bucket["key"]
        for bucket in resp.get("aggregations", {})
        .get("product_type", {})
        .get("values", {})
        .get("buckets", [])
    ]

    category_list = [
        bucket["key"]
        for bucket in resp.get("aggregations", {})
        .get("categories", {})
        .get("values", {})
        .get("buckets", [])
    ]

    # Extract suggestions from ES response
    suggestions = []
    for suggester in resp.get("suggest", {}).values():
        for option in suggester[0].get("options", []):
            suggestions.append(option.get("text"))

    # -----------------------------
    # Format results (documents only)
    # -----------------------------
    results = [
        {
            "id": hit["_id"],
            "score": hit["_score"],
            "name": hit["_source"].get("product_name"),
            "brand": hit["_source"].get("brand"),
            "category": hit["_source"].get("category"),
            "base_price": hit["_source"].get("base_price"),
            "images": [
                i.get("url")
                for i in hit["_source"].get("images", [])
                if isinstance(i, dict)
            ],
        }
        for hit in hits
    ]

    # total_hits = resp.get("hits", {}).get("total", {}).get("value", 0)

    return {
        "total": total_hits,
        "total_docs_after_filter": total_hits,
        "total_docs": total_docs,
        "page": page,
        "size": size,
        "total_pages": (total_hits + size - 1) // size,
        "results": results,
        "suggest": suggestions,
        "facets": {
            "brands": brand_list,
            "categories": category_list,
            "product_type": product_type_list,
        },
    }
