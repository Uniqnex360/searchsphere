import re
from typing import Any, Dict, Optional, List, Tuple
from elasticsearch import Elasticsearch

from app.helpers import get_embedding
from app.services import QDrantCollection

UNITS = ["mm", "cm", "inch", "in", "kg", "g"]
MATERIALS = ["stainless steel", "steel", "plastic", "wood", "cotton"]
BRANDS = [
    "A.Y. McDonald",
    "Aervoe",
    "Avdel",
    "Bunting Bearings",
    "Dewalt",
    "Dodge",
    "Harcofittings",
    "Heli-Coil",
    "Integra",
    "Irwin",
    "Masterfix",
    "Michigan Pneumatic",
    "Midland Industries",
    "Mohawkgroup",
    "Nelson",
    "Nibco",
    "Oilite",
    "POP",
    "Spiralock",
    "Stanley",
    "Thorlabs",
    "Tucker",
]
CATEGORIES = [
    "Accessories",
    "Adapters",
    "Air Tool Accessories",
    "Balancing Valves",
    "Ball Valves",
    "Bars",
    "Brad Point Bits",
    "Breadboards & Accessories",
    "Bronze Sleeve Bushings",
    "By-Pass Valves",
    "Cable Splitters & Signal Amplifiers",
    "Cables",
    "Clamps & Supports",
    "Clip",
    "Coating Materials",
    "Connectors",
    "Couplers & Splitters",
    "Drill Bits and Accessories",
    "Elbow",
    "Fastener Accessories",
    "Ferrules",
    "Fiber Optic Components",
    "Floor Coverings",
    "Hand Tools",
    "Holders",
    "Jack Nut",
    "Laser Levels",
    "Laser Mount",
    "Lenses",
    "Lockbolt Collars",
    "Marking Paints",
    "Metal Drill Bits",
    "Mirrors",
    "Nuts",
    "Optical Accessories",
    "Optical Filters",
    "Paints",
    "Percussion Bits",
    "Pilot Point Bits",
    "Pin & Grommet",
    "Pipe & Pipe Fittings",
    "Plain Bearings",
    "Pneumatic Tools",
    "Power Accessories",
    "Power Tools",
    "PVC Fittings",
    "Retainer",
    "Rivet Nuts",
    "Rivet Tools",
    "Rivets & Lockbolts",
    "Signs & Facility Identification Products",
    "Solid Inserts",
    "Spares & Accessories",
    "Step Stools",
    "Studs, Rivets, Pins",
    "Thread Inserts",
    "Wall Base and Molding",
    "Washers & Retaining Rings",
    "Wires & Cables",
]
SYNONYMS = {
    "ss": ["stainless steel"],
    "tshirt": ["t-shirt", "tee"],
    "phone": ["mobile", "smartphone"],
}


async def query_processor(query: str) -> Dict[str, Any]:
    # 1. normalize base query
    query_lower = query.lower().strip()

    # 2. normalize stuck units: 3.2mm → 3.2 mm
    query_lower = re.sub(
        r"(\d+(\.\d+)?)(mm|cm|inch|in|kg|g)\b",
        r"\1 \3",
        query_lower,
    )

    # 3. extract number + unit pairs
    numbers: List[Tuple[str, str]] = re.findall(
        r"(\d+\.?\d*)\s?(mm|cm|inch|in|kg|g)",
        query_lower,
    )

    # 4. build safe tokens (DO NOT use for ranking alone)
    tokens = re.findall(r"\d+\.?\d+|[a-zA-Z]+", query_lower)

    # 5. build normalized number-unit phrases (IMPORTANT FIX)
    number_unit_phrases: List[str] = []
    for num, unit in numbers:
        number_unit_phrases.append(f"{num}{unit}")  # 3.2mm
        number_unit_phrases.append(f"{num} {unit}")  # 3.2 mm

    # 6. extract metadata
    brands = [b for b in BRANDS if b in query_lower]
    materials = [m for m in MATERIALS if m in query_lower]
    categories = [c for c in CATEGORIES if c in query_lower]

    return {
        "raw": query_lower,
        "numbers": numbers,
        "number_unit_phrases": number_unit_phrases,
        "brand": brands,
        "materials": materials,
        "categories": categories,
        "tokens": tokens,
    }


# -----------------------------
# EXPAND QUERY
# -----------------------------
async def expand_query(parsed: dict) -> list:
    expanded_terms = set(parsed.get("tokens", []))

    # Add synonyms
    for token in parsed.get("tokens", []):
        if token in SYNONYMS:
            expanded_terms.update(SYNONYMS[token])

    # Add materials
    for mat in parsed.get("materials", []):
        expanded_terms.add(mat)

    # Add categories
    for cat in parsed.get("categories", []):
        expanded_terms.add(cat)

    for phrase in parsed.get("number_unit_phrases", []):
        expanded_terms.add(phrase)

    return list(expanded_terms)


# -----------------------------
# BUILD ES QUERY BODY
# -----------------------------
def build_es_query_body(
    user_query: str,
    filters: Optional[Dict[str, Any]] = None,
    expanded_terms: list[str] = [],
) -> Dict[str, Any]:
    should_clauses = []

    for term in expanded_terms:
        should_clauses.append({"match": {"product_name": {"query": term, "boost": 2}}})

    parsed = {"materials": [], "categories": []}  # placeholder
    for mat in parsed["materials"]:
        should_clauses.append(
            {"match": {"attributes.name": {"query": mat, "boost": 3}}}
        )
    for cat in parsed["categories"]:
        should_clauses.append({"match": {"category_name": {"query": cat, "boost": 4}}})

    filter_clauses = []

    if filters:
        if filters.get("brand"):
            filter_clauses.append({"terms": {"brand.keyword": filters["brand"]}})
        if filters.get("category"):
            filter_clauses.append(
                {"terms": {"category_name.keyword": filters["category"]}}
            )
        price_range = {}
        if filters.get("price_min") is not None:
            price_range["gte"] = filters["price_min"]
        if filters.get("price_max") is not None:
            price_range["lte"] = filters["price_max"]
        if price_range:
            filter_clauses.append({"range": {"base_price": price_range}})

    must_clause = []

    if user_query.strip():
        must_clause.append(
            {
                "bool": {
                    "should": [
                        # 1. STRONG BRAND PREFIX MATCH (MOST IMPORTANT)
                        {
                            "prefix": {
                                "brand.keyword": {
                                    "value": user_query.lower(),
                                    "boost": 50,
                                }
                            }
                        },
                        # 2. EXACT BRAND MATCH (VERY STRONG)
                        {
                            "term": {
                                "brand.keyword": {
                                    "value": user_query.lower(),
                                    "boost": 100,
                                }
                            }
                        },
                        # 3. PRODUCT NAME MATCH (NORMAL)
                        {
                            "match_phrase_prefix": {
                                "product_name": {"query": user_query, "boost": 20}
                            }
                        },
                        # 4. GENERAL FALLBACK SEARCH
                        {
                            "multi_match": {
                                "query": user_query,
                                "fields": [
                                    "product_name^5",
                                    "long_description",
                                    "category_name^2",
                                    "vendor_name^3",
                                ],
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            }
        )
    else:
        must_clause.append({"match_all": {}})

    return {
        "query": {
            "bool": {
                "must": must_clause,
                "should": should_clauses,
                "minimum_should_match": 1 if should_clauses else 0,
                "filter": filter_clauses,
            }
        }
    }


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
        must.append({"terms": {"category_name.keyword": filters["category"]}})
    price_range = {}
    if filters.get("price_min") is not None:
        price_range["gte"] = filters["price_min"]
    if filters.get("price_max") is not None:
        price_range["lte"] = filters["price_max"]
    if price_range:
        must.append({"range": {"base_price": price_range}})
    return must


# -----------------------------
# VECTOR SEARCH
# -----------------------------
def vector_search(qdrant, query: str, limit: int = 20, filters: dict = None):
    if not query or not query.strip():
        return []

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
        limit=limit,
        with_payload=True,
    )
    return response.points


# -----------------------------
# MERGE RESULTS
# -----------------------------
def merge_results(keyword_results, vector_results):
    ES_WEIGHT = 0.7
    VECTOR_WEIGHT = 0.3
    results = {}

    for r in keyword_results:
        pid = r["_id"]
        es_score = r.get("_score") or 0
        results[pid] = {
            "data": r["_source"],
            "score": es_score * ES_WEIGHT,
            "score_breakdown": {"es": es_score, "vector": 0},
            "es_order": r.get("_es_order", 0),
        }

    for r in vector_results:
        pid = str(r.id)
        vector_score = r.score
        if pid not in results:
            results[pid] = {
                "data": r.payload,
                "score": vector_score * VECTOR_WEIGHT,
                "score_breakdown": {"es": 0, "vector": vector_score},
                "es_order": float("inf"),
            }
        else:
            results[pid]["score"] += vector_score * VECTOR_WEIGHT
            results[pid]["score_breakdown"]["vector"] = vector_score

    return results


# -----------------------------
# FINAL HYBRID SEARCH (ES + QDRANT)
# -----------------------------
async def get_product_auto_complete_v3(
    es: Elasticsearch,
    qdrant,
    query: str,
    size: int = 50,
    page: int = 1,
    filters: Optional[Dict[str, Any]] = None,
    sort_by: str = "relevance",  # relevance | product_name | base_price
    sort_order: str = "desc",
    index: str = "product_vector",
):
    query_dict = await query_processor(query)
    print("query dict", query_dict)
    expanded_query = await expand_query(query_dict)
    print("expended query", expanded_query)

    es_query_body = build_es_query_body(
        query, filters=filters, expanded_terms=expanded_query
    )

    # -----------------------------
    # Elasticsearch sort (null-safe at ES level)
    # -----------------------------
    es_sort = []
    if sort_by == "product_name":
        es_sort.append(
            {
                "product_name.keyword": {
                    "order": sort_order,
                    "missing": "_last",  # nulls come last
                }
            }
        )
    elif sort_by == "base_price":
        es_sort.append(
            {
                "base_price": {
                    "order": sort_order,
                    "missing": "_last",  # null prices come last
                }
            }
        )
    else:  # relevance
        es_sort.append({"_score": {"order": sort_order}})

    # -----------------------------
    # Execute ES query
    # -----------------------------
    body = {
        "from": (page - 1) * size,
        "size": size,
        "_source": [
            "product_name",
            "brand",
            "category_name",
            "taxonomy",
            "base_price",
            "images.url",
        ],
        "query": es_query_body["query"],
        "sort": es_sort,
    }

    es_resp = es.search(index=index, body=body)
    total_docs = es.count(index=index)["count"]
    total_docs_after_filter = es_resp["hits"]["total"]["value"]
    keyword_hits = es_resp.get("hits", {}).get("hits", [])

    # Vector search
    vector_results = vector_search(qdrant, query, limit=size, filters=filters)

    # Merge ES + Vector
    merged = merge_results(keyword_hits, vector_results)

    # Format results
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
            }
        )

    return {
        "total_docs": total_docs,
        "total_docs_after_filter": total_docs_after_filter or len(results),
        "page": page,
        "size": size,
        "total_pages": (total_docs_after_filter + size - 1) // size,
        "results": results,
    }
