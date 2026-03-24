import re
from typing import Any, Dict, Optional
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


async def query_processor(query: str) -> dict:
    query_lower = query.lower().strip()

    # Extract numbers with units
    numbers = re.findall(r"(\d+\.?\d*)\s?(mm|cm|inch|in|kg|g)", query_lower)

    # Extract materials and categories
    brands = [b for b in BRANDS if b in query_lower]
    materials = [m for m in MATERIALS if m in query_lower]
    categories = [c for c in CATEGORIES if c in query_lower]

    # Split tokens
    tokens = query_lower.split()

    return {
        "raw": query_lower,
        "numbers": numbers,
        "brand": brands,
        "materials": materials,
        "categories": categories,
        "tokens": tokens,
    }


async def expand_query(parsed: dict) -> list:
    # Start with the original tokens
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

    return list(expanded_terms)


def build_es_query(
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
        # Multi-select brand
        if filters.get("brand"):
            filter_clauses.append({"terms": {"brand.keyword": filters["brand"]}})

        # Multi-select category
        if filters.get("category"):
            filter_clauses.append(
                {"terms": {"category_name.keyword": filters["category"]}}
            )

        # Price range
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
                "multi_match": {
                    "query": user_query,
                    "fields": ["product_name^3", "long_description"],
                }
            }
        )
    else:
        must_clause.append({"match_all": {}})

    return {
        "bool": {
            "must": must_clause,
            "should": should_clauses,
            "minimum_should_match": 1 if should_clauses else 0,
            "filter": filter_clauses,
        }
    }


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


def merge_results(keyword_results, vector_results, debug: bool = True):
    ES_WEIGHT = 0.7
    VECTOR_WEIGHT = 0.3
    results = {}

    if debug:
        print("\n========== MERGE DEBUG START ==========")
        print(f"ES Results Count      : {len(keyword_results)}")
        print(f"Vector Results Count  : {len(vector_results)}")

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
            "es_order": r.get("_es_order", 0),
        }

    if debug and vector_results:
        print("\n--- Vector Results ---")

    for r in vector_results:
        pid = str(r.id)
        vector_score = r.score

        if pid not in results:
            if debug:
                print(
                    f"[VECTOR ONLY] ID={pid} | Vector Score={vector_score:.4f} | Weighted={vector_score * VECTOR_WEIGHT:.4f}"
                )

            results[pid] = {
                "data": r.payload,
                "score": vector_score * VECTOR_WEIGHT,
                "score_breakdown": {"es": 0, "vector": vector_score},
                "es_order": float("inf"),
            }
        else:
            if debug:
                print(
                    f"[MERGED] ID={pid} | Vector Score={vector_score:.4f} | Added={vector_score * VECTOR_WEIGHT:.4f}"
                )

            results[pid]["score"] += vector_score * VECTOR_WEIGHT
            results[pid]["score_breakdown"]["vector"] = vector_score

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


def build_es_query_body(
    user_query: str,
    filters: Optional[Dict[str, Any]] = None,
    expanded_terms: list[str] = [],
) -> Dict[str, Any]:
    """Builds the Elasticsearch query body (unchanged)."""
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
                "multi_match": {
                    "query": user_query,
                    "fields": ["product_name^3", "long_description"],
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


async def get_product_auto_complete_v3(
    es: Elasticsearch,
    qdrant,
    query: str,
    size: int = 50,
    filters: Optional[Dict[str, Any]] = None,
    sort_by: str = "relevance",
    sort_order: str = "desc",
    index: str = "product_vector",
):
    query_dict = await query_processor(query)
    expanded_query = await expand_query(query_dict)

    # Build the ES query body
    es_query_body = build_es_query_body(
        query, filters=filters, expanded_terms=expanded_query
    )

    # --- Execute the query in ES to get total_docs and results ---
    es_resp = es.search(index=index, body=es_query_body, size=size)

    total_docs = es_resp["hits"]["total"]["value"] if "total" in es_resp["hits"] else 0
    total_docs_after_filter = len(es_resp["hits"]["hits"])
    keyword_results = es_resp["hits"]["hits"]

    # Vector search
    vector_results = vector_search(qdrant, query, limit=size, filters=filters)

    # Merge ES + vector results
    merged = merge_results(keyword_results, vector_results)

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
                "es_order": item.get("es_order", float("inf")),
            }
        )

    reverse = sort_order == "desc"

    def null_safe_key(val, reverse=False):
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

    for r in results:
        r.pop("es_order", None)

    return {
        "total_docs": total_docs,
        "total_docs_after_filter": total_docs_after_filter,
        "results": results,
    }
