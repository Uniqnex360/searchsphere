from typing import Optional, List
from elasticsearch import Elasticsearch
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func, desc
from sqlalchemy.orm import selectinload
from sqlalchemy import select, func, desc, asc, cast, String, case
from sqlalchemy.sql.sqltypes import String

from fastapi import APIRouter, Depends, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from app.models import Product, Category, ProductSearchResult, Brand
from app.services import (
    ESCollection,
    ElasticsearchService,
    autocomplete_products,
    autocomplete_product_vector,
    autocomplete_with_es_qdrant,
    get_product_auto_complete_v3,
    get_product_auto_complete_v4,
    create_product_mapping,
    sync_products_to_es,
    sync_product_suggest_data_es_v6,
    get_product_list_v6,
)
from app.es_client import get_es
from app.database import get_session
from app.services import get_qdrant_client

router = APIRouter()


@router.get("/product/auto-complete/")
async def autocomplate_product(q: str, es: Elasticsearch = Depends(get_es)):
    data = await autocomplete_products(es, q)
    return {"total": 0, "data": data}


@router.get("/product/vector/auto-complete/")
async def autocomplate_product_vector(
    q: str = "",
    brand_: Optional[List[str]] = Query(None, alias="brand[]"),
    category_: Optional[List[str]] = Query(None, alias="category[]"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    es: Elasticsearch = Depends(get_es),
    qdrant: Elasticsearch = Depends(get_qdrant_client),
    sort_by: str | None = None,
    sort_order: str = "desc",
):

    filters = {
        "brand": brand_,
        "category": category_,
        "price_min": price_min,
        "price_max": price_max,
    }
    data = await autocomplete_with_es_qdrant(
        es, qdrant, q, filters=filters, sort_by=sort_by, sort_order=sort_order
    )
    return {"total": len(data), "data": data}


@router.get("/product/filter-meta/")
async def get_product_filter_meta(db: AsyncSession = Depends(get_session)):

    # -------------------
    # 1. Get unique brands in ascending order
    # -------------------
    brand_stmt = (
        select(Brand.brand_name)
        .join(Product, Product.brand_id == Brand.id)
        .distinct()
        .order_by(Brand.brand_name.asc())
    )
    brand_result = await db.exec(brand_stmt)
    brands = [row[0] for row in brand_result.all()]

    # -------------------
    # 2. Get unique categories in ascending order
    # -------------------
    category_stmt = (
        select(func.distinct(Category.name))
        .select_from(Product)
        .join(Category, Product.category_id == Category.id)
        .order_by(Category.name.asc())
    )
    category_result = await db.execute(category_stmt)
    categories = [name for name in category_result.scalars().all()]

    # -------------------
    # 3. Get min & max price
    # -------------------
    price_stmt = select(func.min(Product.base_price), func.max(Product.base_price))
    price_result = await db.execute(price_stmt)
    min_price, max_price = price_result.one()

    # -------------------
    # 4. Generate price ranges
    # -------------------
    price_ranges = []
    if min_price is not None and max_price is not None:
        steps = 4
        step_size = (max_price - min_price) / steps

        current = min_price
        for _ in range(steps):
            price_ranges.append(
                {"min": round(current, 2), "max": round(current + step_size, 2)}
            )
            current += step_size

    return {"brands": brands, "categories": categories, "price_ranges": price_ranges}


@router.get("/product/detail/{id}/")
async def get_product_detail(id: int, db: AsyncSession = Depends(get_session)):
    result = await db.execute(
        select(Product)
        .where(Product.id == id)
        .options(
            selectinload(Product.images),
            selectinload(Product.features),
            selectinload(Product.attributes),
            selectinload(Product.videos),
            selectinload(Product.documents),
            selectinload(Product.category),
            selectinload(Product.industry),
            selectinload(Product.brand),
        )
    )
    product = result.scalar_one_or_none()

    if not product:
        return JSONResponse(
            status_code=404, content={"success": False, "error": "Product not found"}
        )

    # Convert SQLAlchemy object to JSON-serializable dict
    product_data = jsonable_encoder(product)

    # Convert related objects
    product_data["images"] = [jsonable_encoder(img) for img in product.images]
    product_data["features"] = [jsonable_encoder(f) for f in product.features]
    product_data["attributes"] = [jsonable_encoder(a) for a in product.attributes]
    product_data["videos"] = [jsonable_encoder(v) for v in product.videos]
    product_data["documents"] = [jsonable_encoder(d) for d in product.documents]
    product_data["category"] = (
        jsonable_encoder(product.category) if product.category else None
    )
    product_data["industry"] = (
        jsonable_encoder(product.industry) if product.industry else None
    )
    product_data["brand"] = jsonable_encoder(product.brand) if product.brand else None

    return {"success": True, "data": product_data}


async def save_search_result(
    es: Elasticsearch,
    session: AsyncSession,
    query_data: dict,
    data: dict,
    url: str,
    debug: bool = True,
):
    query = (query_data.get("q") or "").strip()
    if not query:
        return

    debug_tokens = {}

    # -----------------------------
    # 🔥 ANALYZE WITH EXPLAIN
    # -----------------------------
    async def analyze_with_explain(analyzer_name: str, text: str):
        try:

            resp = es.indices.analyze(
                index=ESCollection.PRODUCT_V2.value,
                body={
                    "analyzer": analyzer_name,
                    "text": text,
                    "explain": True,
                },
            )

            # Tokenizer output
            tokenizer_tokens = [
                t["token"]
                for t in resp.get("detail", {}).get("tokenizer", {}).get("tokens", [])
            ]

            # Token filter stages
            filters_output = {}
            for f in resp.get("detail", {}).get("tokenfilters", []):
                fname = f.get("name")
                tokens = [t["token"] for t in f.get("tokens", [])]
                filters_output[fname] = tokens

            # Final tokens
            final_tokens = [t["token"] for t in resp.get("tokens", [])]

            return {
                "tokenizer": tokenizer_tokens,
                "filters": filters_output,
                "final": final_tokens,
            }

        except Exception as e:
            return None

    # -----------------------------
    # 🔥 RUN DEBUG ANALYSIS
    # -----------------------------
    if debug and query:
        try:
            debug_tokens = {
                "product_name_index": await analyze_with_explain(
                    "custom_text_analyzer", query
                ),
                "product_name_search": await analyze_with_explain(
                    "custom_search_analyzer", query
                ),
                "product_name_ngram": await analyze_with_explain(
                    "autocomplete_analyzer", query
                ),
            }

            # Remove failed ones
            debug_tokens = {k: v for k, v in debug_tokens.items() if v is not None}

        except Exception as e:

            debug_tokens = {"error": str(e)}

    # -----------------------------
    # 💾 SAVE TO DB
    # -----------------------------
    query_data["tokens"] = debug_tokens if debug else None
    try:
        obj = ProductSearchResult(
            query=query_data,
            result=data,
            url=str(url),
            total_result=data.get("total_docs_after_filter", None),
        )

        session.add(obj)
        await session.commit()

    except Exception as e:
        await session.rollback()


@router.get("/product/v3/auto-complete/")
async def autocomplate_product_vector(
    request: Request,
    background_tasks: BackgroundTasks,
    q: str = "",
    brand_: Optional[List[str]] = Query(None, alias="brand[]"),
    category_: Optional[List[str]] = Query(None, alias="category[]"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    es: Elasticsearch = Depends(get_es),
    qdrant: Elasticsearch = Depends(get_qdrant_client),
    session: AsyncSession = Depends(get_session),
    sort_by: str | None = None,
    sort_order: str = "desc",
    page: int = 1,
):

    filters = {
        "brand": brand_,
        "category": category_,
        "price_min": price_min,
        "price_max": price_max,
    }
    data = await get_product_auto_complete_v3(
        es,
        qdrant,
        q,
        filters=filters,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
    )

    query_payload = {
        "q": q,
        "processed_query": data.get("processed_query"),
        "filters": filters,
        "page": page,
    }

    background_tasks.add_task(
        save_search_result,
        session,
        query_payload,
        data,
        request.headers.get("X-FE-URL", str(request.url)),
    )
    return {"total": len(data), "data": data}


@router.post("/product/es-mapping/create/")
async def create_elastic_search_product_mapping(
    es: Elasticsearch = Depends(get_es),
):
    try:
        create_product_mapping(es)
    except Exception as e:
        return {"success": False, "error": str(e)}
    return {"success": True}


@router.get("/product/v4/auto-complete/")
async def autocomplate_product_vector(
    request: Request,
    background_tasks: BackgroundTasks,
    q: str = "",
    brand_: Optional[List[str]] = Query(None, alias="brand[]"),
    product_type_: Optional[List[str]] = Query(None, alias="product_type[]"),
    category_: Optional[List[str]] = Query(None, alias="category[]"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    es: Elasticsearch = Depends(get_es),
    qdrant: Elasticsearch = Depends(get_qdrant_client),
    session: AsyncSession = Depends(get_session),
    sort_by: str | None = None,
    sort_order: str = "desc",
    page: int = 1,
):

    filters = {
        "brand": brand_,
        "product_type": product_type_,
        "category": category_,
        "price_min": price_min,
        "price_max": price_max,
    }
    data = await get_product_auto_complete_v4(
        es,
        q,
        brand=filters.get("brand"),
        product_type=filters.get("product_type"),
        category=filters.get("category"),
        min_price=filters.get("price_min"),
        max_price=filters.get("price_max"),
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
    )

    query_payload = {
        "q": q,
        "processed_query": data.get("processed_query"),
        "filters": filters,
        "page": page,
    }

    background_tasks.add_task(
        save_search_result,
        es,
        session,
        query_payload,
        data,
        request.headers.get("X-FE-URL", str(request.url)),
    )
    return {"total": len(data), "data": data}


def get_pagination_params(request: Request):
    try:
        page = int(request.query_params.get("page", 1))
        limit = int(request.query_params.get("limit", 50))
    except ValueError:
        page, limit = 1, 40

    page = max(page, 1)
    limit = min(max(limit, 1), 100)

    return page, limit


from app.services import sync_products_to_es_v5


@router.get("/product/sync/es/")
async def sync_product_with_elastic_serch(
    session: AsyncSession = Depends(get_session),
    es: Elasticsearch = Depends(get_es),
):

    es_service = ElasticsearchService(es, ESCollection.PRODUCT_V5.value)

    await sync_products_to_es_v5(session=session, es_service=es_service, batch_size=30)

    return {"success": True}


@router.get("/product/search/keywords/")
async def get_product_search_keywords_list(
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    # Pagination
    page, limit = get_pagination_params(request)
    offset = (page - 1) * limit

    # Sorting & search params
    sort_by = request.query_params.get("sort_by", "created_at")
    order = request.query_params.get("order", "desc").lower()
    search_q = request.query_params.get("search")

    # Keyword extraction from JSON field
    # keyword_col = cast(ProductSearchResult.query["q"], String)
    keyword_col = func.lower(cast(ProductSearchResult.query["q"], String))

    # Base aggregate
    base_query = (
        select(
            keyword_col.label("q"),
            ProductSearchResult.total_result,
            func.count().label("search_count"),
            func.max(ProductSearchResult.created_at).label("created_at"),
            func.max(ProductSearchResult.url).label("url"),
        )
        .where(ProductSearchResult.is_active == True)
        .group_by(keyword_col, ProductSearchResult.total_result)
    )

    # Apply search filter
    if search_q:
        base_query = base_query.where(keyword_col.ilike(f"%{search_q}%"))

    # Subquery for pagination and sorting
    subquery = base_query.subquery()
    cols = subquery.c

    # Total and unique count for this search
    count_query = select(
        func.count().label("total_count"),
        func.count(func.distinct(keyword_col)).label("unique_count"),
    ).select_from(ProductSearchResult)
    if search_q:
        count_query = count_query.where(keyword_col.ilike(f"%{search_q}%"))

    count_result = await db.exec(count_query)
    count_data = count_result.one()
    total_count = count_data.total_count
    unique_count = count_data.unique_count

    # Sorting logic
    sort_column_map = {
        "q": cols.q,
        "total_result": cols.total_result,
        "search_count": cols.search_count,
        "created_at": cols.created_at,
    }
    sort_column = sort_column_map.get(sort_by, cols.created_at)

    order_expressions = []
    if sort_by == "q":
        empty_last = case((cols.q == "", 1), else_=0)
        if order == "asc":
            order_expressions = [empty_last.asc(), cols.q.asc()]
        else:
            order_expressions = [empty_last.asc(), cols.q.desc()]
    else:
        order_expressions = (
            [asc(sort_column).nulls_last()]
            if order == "asc"
            else [desc(sort_column).nulls_last()]
        )

    # Final paginated query
    final_query = (
        select(
            cols.q,
            cols.total_result,
            cols.url,
            cols.search_count,
            cols.created_at,
        )
        .order_by(*order_expressions)
        .offset(offset)
        .limit(limit)
    )

    result = await db.exec(final_query)
    rows = result.all()

    data = [
        {
            "q": row.q,
            "total_result": row.total_result,
            "url": row.url,
            "search_count": row.search_count,
            "created_at": row.created_at,
        }
        for row in rows
    ]

    return {
        "data": data,
        "meta": {
            "page": page,
            "limit": limit,
            "total": total_count,
            "unique": unique_count,
            "pages": (unique_count + limit - 1) // limit,
        },
    }


@router.post("/product/v6/mapping/")
async def update_product_data_v6(
    es: Elasticsearch = Depends(get_es), session: AsyncSession = Depends(get_session)
):
    await sync_product_suggest_data_es_v6(es, session)
    return {}


@router.get("/product/v6/list/")
async def product_list_v6(
    request: Request,
    background_tasks: BackgroundTasks,
    q: str = "",
    brand_: Optional[List[str]] = Query(None, alias="brand[]"),
    product_type_: Optional[List[str]] = Query(None, alias="product_type[]"),
    category_: Optional[List[str]] = Query(None, alias="category[]"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    es: Elasticsearch = Depends(get_es),
    session: AsyncSession = Depends(get_session),
    sort_by: str | None = None,
    sort_order: str = "desc",
    page: int = 1,
):

    filters = {
        "brand": brand_,
        "product_type": product_type_,
        "category": category_,
        "price_min": price_min,
        "price_max": price_max,
    }
    data = await get_product_list_v6(
        es,
        q,
        brand=filters.get("brand"),
        product_type=filters.get("product_type"),
        category=filters.get("category"),
        min_price=filters.get("price_min"),
        max_price=filters.get("price_max"),
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
    )

    query_payload = {
        "q": q,
        "processed_query": data.get("processed_query"),
        "filters": filters,
        "page": page,
    }

    background_tasks.add_task(
        save_search_result,
        es,
        session,
        query_payload,
        data,
        request.headers.get("X-FE-URL", str(request.url)),
    )
    return {"total": len(data), "data": data}


# @router.get("/product/v6/auto-complete/")
# async def get_product_auto_complete_v6(
#     q: str = Query(..., min_length=1),
#     size: int = 10,
#     es: Elasticsearch = Depends(get_es),
# ):
#     index_name = ESCollection.PRODUCT_AUTO_SUGGEST_V6.value
#     q_clean = q.strip().lower()

#     if not q_clean:
#         return {"success": True, "query": q, "count": 0, "results": []}

#     # Elasticsearch query: match brand_name, brand_category, brand_category_product_type
#     es_query = {
#         "size": 100,  # fetch extra to handle duplicates
#         "_source": [
#             "brand_name",
#             "brand_category",
#             "brand_category_product_type",
#             "brand_category_product_type_attribute",
#         ],
#         "query": {
#             "bool": {
#                 "should": [
#                     {"match_phrase_prefix": {"brand_name": {"query": q}}},
#                     {"match_phrase_prefix": {"brand_category": {"query": q}}},
#                     {
#                         "match_phrase_prefix": {
#                             "brand_category_product_type": {"query": q}
#                         }
#                     },
#                     {
#                         "match_phrase_prefix": {
#                             "brand_category_product_type_attribute": {"query": q}
#                         }
#                     },
#                 ]
#             }
#         },
#     }

#     try:
#         response = es.search(index=index_name, body=es_query)
#     except Exception as e:
#         return {
#             "success": False,
#             "error": str(e),
#             "query": q,
#             "count": 0,
#             "results": [],
#         }

#     hits = response.get("hits", {}).get("hits", [])
#     suggestions = []
#     seen = set()

#     for hit in hits:
#         source = hit.get("_source", {})
#         all_strings = []

#         # Add brand name
#         brand_name = source.get("brand_name", "").strip()
#         if brand_name:
#             all_strings.append(brand_name)

#         # Add brand_category + brand_category_product_type
#         for field in [
#             "brand_category",
#             "brand_category_product_type",
#             "brand_category_product_type_attribute",
#         ]:
#             for entry in source.get(field, []):
#                 if entry:
#                     all_strings.append(entry.strip())

#         # Filter by query anywhere in the string
#         for s in all_strings:
#             s_lower = s.lower()
#             if q_clean in s_lower and s not in seen:
#                 suggestions.append({"text": s})
#                 seen.add(s)
#             if len(suggestions) >= size:
#                 break
#         if len(suggestions) >= size:
#             break

#     return {
#         "success": True,
#         "query": q,
#         "count": len(suggestions),
#         "results": suggestions[:size],
#     }


@router.get("/product/v6/auto-complete/")
async def get_product_auto_complete_v6(
    q: str = Query(..., min_length=1),
    size: int = 10,
    es: Elasticsearch = Depends(get_es),
):
    index_name = ESCollection.PRODUCT_AUTO_SUGGEST_V6.value
    q_clean = q.strip().lower()

    if not q_clean:
        return {"success": True, "query": q, "count": 0, "results": []}

    # 🔥 split query words (for multi-word ranking)
    q_words = q_clean.split()

    # Elasticsearch query (prefix + fuzzy)
    es_query = {
        "size": 200,
        "_source": [
            "brand_name",
            "brand_category",
            "brand_category_product_type",
            "brand_category_product_type_attribute",
        ],
        "query": {
            "bool": {
                "should": [
                    # ✅ prefix queries
                    {"match_phrase_prefix": {"brand_name": {"query": q}}},
                    {"match_phrase_prefix": {"brand_category": {"query": q}}},
                    {
                        "match_phrase_prefix": {
                            "brand_category_product_type": {"query": q}
                        }
                    },
                    {
                        "match_phrase_prefix": {
                            "brand_category_product_type_attribute": {"query": q}
                        }
                    },
                    # ✅ fuzzy fallback (improved)
                    {
                        "match": {
                            "brand_name": {
                                "query": q,
                                "fuzziness": "AUTO",
                                "operator": "and",  # 🔥 important
                                "boost": 0.6,
                            }
                        }
                    },
                    {
                        "match": {
                            "brand_category": {
                                "query": q,
                                "fuzziness": "AUTO",
                                "operator": "and",  # 🔥 important
                                "boost": 0.4,
                            }
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
    }

    try:
        response = es.search(index=index_name, body=es_query)
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query": q,
            "count": 0,
            "results": [],
        }

    hits = response.get("hits", {}).get("hits", [])
    suggestions = []
    seen = set()

    for hit in hits:
        source = hit.get("_source", {})
        score = hit.get("_score", 0)
        all_strings = []

        # brand name
        brand_name = source.get("brand_name", "").strip()
        if brand_name:
            all_strings.append(brand_name)

        # other fields
        for field in [
            "brand_category",
            "brand_category_product_type",
            "brand_category_product_type_attribute",
        ]:
            for entry in source.get(field, []):
                if entry:
                    all_strings.append(entry.strip())

        # 🔥 filtering + ranking logic
        for s in all_strings:
            s_lower = s.lower()

            # ❌ remove exact duplicate (e.g., "3M")
            if s_lower == q_clean:
                continue

            # 🔥 count matching words
            word_match_count = sum(1 for w in q_words if w in s_lower)

            if (
                word_match_count > 0  # at least one word matches
                or score > 1  # fuzzy fallback
            ) and s not in seen:

                suggestions.append(
                    {
                        "text": s,
                        "score": score,
                        "word_match": word_match_count,
                    }
                )
                seen.add(s)

            if len(suggestions) >= size * 3:  # buffer for sorting
                break

        if len(suggestions) >= size * 3:
            break

    # 🔥 smart sorting (VERY IMPORTANT)
    suggestions = sorted(
        suggestions,
        key=lambda x: (
            -x["word_match"],  # more words matched → higher
            -x["score"],  # ES relevance
            x["text"],
        ),
    )

    # 🔥 final response
    results = [{"text": s["text"]} for s in suggestions[:size]]

    return {
        "success": True,
        "query": q,
        "count": len(results),
        "results": results,
    }
