from typing import Optional, List
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func, desc
from sqlalchemy.orm import selectinload
from sqlalchemy import select, func, desc, asc, cast, String, case, and_
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
    update_product_view_count,
    increment_search_popularity,
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


import time


@router.get("/product/detail/{id}/")
async def get_product_detail(
    id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_session),
    es=Depends(get_es),
):

    db_time = time.time()
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

    print("db time", time.time() - db_time)

    product = result.scalar_one_or_none()

    if not product:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "Product not found"},
        )

    # -------------------------
    # Serialize safely
    # -------------------------
    product_data = jsonable_encoder(product)

    product_data["images"] = [jsonable_encoder(i) for i in product.images]
    product_data["features"] = [jsonable_encoder(i) for i in product.features]
    product_data["attributes"] = [jsonable_encoder(i) for i in product.attributes]
    product_data["videos"] = [jsonable_encoder(i) for i in product.videos]
    product_data["documents"] = [jsonable_encoder(i) for i in product.documents]

    product_data["category"] = (
        jsonable_encoder(product.category) if product.category else None
    )
    product_data["industry"] = (
        jsonable_encoder(product.industry) if product.industry else None
    )
    product_data["brand"] = jsonable_encoder(product.brand) if product.brand else None

    # -------------------------
    # Background task
    # -------------------------
    background_tasks.add_task(
        update_product_view_count,
        es,
        {
            "brand": product.brand.brand_name if product.brand else None,
            "sku": getattr(product, "sku", None),
            "mpn": getattr(product, "mpn", None),
            "product_name": product.product_name,
        },
    )

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

    es_service = ElasticsearchService(es, ESCollection.PRODUCT_V7.value)

    await sync_products_to_es_v5(session=session, es_service=es_service, batch_size=100)

    return {"success": True}


# -----------------------------
# DATE RANGE HELPER
# -----------------------------
def get_day_range(date: datetime):
    start = datetime(date.year, date.month, date.day)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return start, end


@router.get("/product/search/keywords/")
async def get_product_search_keywords_list(
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    # -------------------------
    # Pagination
    # -------------------------
    page = int(request.query_params.get("page", 1))
    limit = int(request.query_params.get("limit", 50))
    offset = (page - 1) * limit

    # -------------------------
    # Filters
    # -------------------------
    sort_by = request.query_params.get("sort_by", "created_at")
    order = request.query_params.get("order", "desc").lower()
    search_q = request.query_params.get("search")
    result_type = request.query_params.get("result_type", "all")

    # 👉 NEW DATE FILTERS
    start_date_str = request.query_params.get("start_date")
    end_date_str = request.query_params.get("end_date")

    date_filters = []

    if start_date_str:
        start_dt = datetime.fromisoformat(start_date_str)
        start_dt, _ = get_day_range(start_dt)
        date_filters.append(ProductSearchResult.created_at >= start_dt)

    if end_date_str:
        end_dt = datetime.fromisoformat(end_date_str)
        _, end_dt = get_day_range(end_dt)
        date_filters.append(ProductSearchResult.created_at <= end_dt)

    # -------------------------
    # Keyword extraction
    # -------------------------
    keyword_col = func.lower(cast(ProductSearchResult.query["q"], String))

    # =========================================================
    # 1. BASE QUERY (GROUPED)
    # =========================================================
    base_query = select(
        keyword_col.label("q"),
        ProductSearchResult.total_result.label("total_result"),
        func.count().label("search_count"),
        func.max(ProductSearchResult.created_at).label("created_at"),
        func.max(ProductSearchResult.url).label("url"),
    ).where(ProductSearchResult.is_active == True)

    # 👉 Apply date filters
    if date_filters:
        base_query = base_query.where(and_(*date_filters))

    if search_q:
        base_query = base_query.where(keyword_col.ilike(f"%{search_q}%"))

    if result_type == "zero":
        base_query = base_query.where(ProductSearchResult.total_result == 0)
    elif result_type == "non_zero":
        base_query = base_query.where(ProductSearchResult.total_result > 0)

    base_query = base_query.group_by(
        keyword_col,
        ProductSearchResult.total_result,
    )

    subquery = base_query.subquery()
    cols = subquery.c

    # =========================================================
    # 2. UNIQUE COUNT
    # =========================================================
    unique_count_query = select(func.count()).select_from(subquery)
    unique_count = (await db.exec(unique_count_query)).scalar()

    # =========================================================
    # 3. TOTAL COUNT (RAW)
    # =========================================================
    total_count_query = (
        select(func.count())
        .select_from(ProductSearchResult)
        .where(ProductSearchResult.is_active == True)
    )

    # 👉 Apply date filters
    if date_filters:
        total_count_query = total_count_query.where(and_(*date_filters))

    if search_q:
        total_count_query = total_count_query.where(keyword_col.ilike(f"%{search_q}%"))

    if result_type == "zero":
        total_count_query = total_count_query.where(
            ProductSearchResult.total_result == 0
        )
    elif result_type == "non_zero":
        total_count_query = total_count_query.where(
            ProductSearchResult.total_result > 0
        )

    total_count = (await db.exec(total_count_query)).scalar()

    # =========================================================
    # 4. SORTING
    # =========================================================
    sort_column_map = {
        "q": cols.q,
        "total_result": cols.total_result,
        "search_count": cols.search_count,
        "created_at": cols.created_at,
    }

    sort_column = sort_column_map.get(sort_by, cols.created_at)

    if sort_by == "q":
        empty_last = case((cols.q == "", 1), else_=0)
        order_expressions = (
            [empty_last.asc(), cols.q.asc()]
            if order == "asc"
            else [empty_last.asc(), cols.q.desc()]
        )
    else:
        order_expressions = (
            [asc(sort_column).nulls_last()]
            if order == "asc"
            else [desc(sort_column).nulls_last()]
        )

    # =========================================================
    # 5. DATA QUERY
    # =========================================================
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

    rows = (await db.exec(final_query)).all()

    data = [
        {
            "q": r.q,
            "total_result": r.total_result,
            "url": r.url,
            "search_count": r.search_count,
            "created_at": r.created_at,
        }
        for r in rows
    ]

    # =========================================================
    # RESPONSE
    # =========================================================
    return {
        "data": data,
        "meta": {
            "page": page,
            "limit": limit,
            "total": total_count,  # raw count
            "unique": unique_count,  # grouped count
            "pages": (unique_count + limit - 1) // limit,
        },
    }


@router.post("/product/v6/mapping/")
async def update_product_data_v6(
    es: Elasticsearch = Depends(get_es),
    session: AsyncSession = Depends(get_session),
    start_id: int = 1,
):
    await sync_product_suggest_data_es_v6(es, session, start_id=start_id)
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
    if q:
        product_ids = [item["id"] for item in data["results"]]

        background_tasks.add_task(
            increment_search_popularity, es, ESCollection.PRODUCT_V7.value, product_ids
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


# @router.get("/product/v6/auto-complete/")
# async def get_product_auto_complete_v6(
#     q: str = Query(..., min_length=1),
#     size: int = 10,
#     es: Elasticsearch = Depends(get_es),
# ):
#     index_name = ESCollection.PRODUCT_AUTO_SUGGEST_V7.value
#     q_clean = q.strip().lower()

#     if not q_clean:
#         return {"success": True, "query": q, "count": 0, "results": []}

#     # split query into words for ranking
#     q_words = q_clean.split()

#     # Elasticsearch query using search_as_you_type fields
#     es_query = {
#         "size": 200,
#         "_source": [
#             "brand_name",
#             "brand_category",
#             "brand_category_product_type",
#             "brand_category_product_type_attribute",
#         ],
#         "query": {
#             "multi_match": {
#                 "query": q,
#                 "type": "bool_prefix",
#                 "fields": [
#                     "brand_name.autocomplete",
#                     "brand_category.autocomplete",
#                     "brand_category_product_type.autocomplete",
#                     "brand_category_product_type_attribute.keyword",
#                 ],
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
#         source = hit.get("_source", {}) or {}
#         score = hit.get("_score", 0) or 0
#         all_strings = []

#         # brand_name
#         brand_name = (source.get("brand_name") or "").strip()
#         if brand_name:
#             all_strings.append(brand_name)

#         # other fields safely
#         for field in [
#             "brand_category",
#             "brand_category_product_type",
#             "brand_category_product_type_attribute",
#         ]:
#             field_values = source.get(field) or []
#             for entry in field_values:
#                 if entry:
#                     all_strings.append(entry.strip())

#         # filtering + ranking
#         for s in all_strings:
#             s_lower = s.lower()

#             # skip exact query duplicates
#             if s_lower == q_clean:
#                 continue

#             # count matching words
#             word_match_count = sum(1 for w in q_words if w in s_lower)

#             if (word_match_count > 0 or score > 1) and s not in seen:
#                 suggestions.append(
#                     {"text": s, "score": score, "word_match": word_match_count}
#                 )
#                 seen.add(s)

#             if len(suggestions) >= size * 3:
#                 break

#         if len(suggestions) >= size * 3:
#             break

#     # smart sorting: match words → ES score → alphabetical
#     suggestions = sorted(
#         suggestions,
#         key=lambda x: (-x["word_match"], -x["score"], x["text"]),
#     )

#     # return top `size` results
#     results = [{"text": s["text"]} for s in suggestions[:size]]

#     return {"success": True, "query": q, "count": len(results), "results": results}


@router.get("/product/v6/auto-complete/")
async def get_product_auto_complete_v6(
    q: str = Query(..., min_length=1),
    size: int = 10,
    es: Elasticsearch = Depends(get_es),
):
    index_name = ESCollection.PRODUCT_AUTO_SUGGEST_V7.value
    q_clean = q.strip().lower()

    if not q_clean:
        return {
            "success": True,
            "query": q,
            "primary_results": [],
            "fallback_results": [],
        }

    q_words = q_clean.split()

    # ---------------------------------------------------------
    # 🔹 1. OPTIMIZED SEARCH QUERY (Now using .autocomplete for attributes)
    # ---------------------------------------------------------
    search_query = {
        "size": 200,
        "query": {
            "bool": {
                "should": [
                    {
                        # TIER 1: Exact Prefix matching across all fields
                        "multi_match": {
                            "query": q_clean,
                            "type": "bool_prefix",
                            "fields": [
                                "brand_category_product_type_attribute.autocomplete^25",  # NEW: Using autocomplete field
                                "brand_category_product_type.autocomplete^15",
                                "brand_category.autocomplete^10",
                                "brand_name.autocomplete^5",
                            ],
                            "boost": 10,
                        }
                    },
                    {
                        # TIER 2: Fuzzy matching for typos
                        "multi_match": {
                            "query": q_clean,
                            "fields": [
                                "brand_category_product_type_attribute^10",
                                "brand_category_product_type^5",
                                "brand_name^10",
                            ],
                            "fuzziness": "AUTO",
                            "prefix_length": 1,
                        }
                    },
                ]
            }
        },
    }

    try:
        response = es.search(index=index_name, body=search_query)
        hits = response.get("hits", {}).get("hits", [])
    except Exception as e:
        return {"success": False, "error": str(e)}

    # ---------------------------------------------------------
    # 🔹 2. THREE-TIER BUCKETING & FILTERING
    # ---------------------------------------------------------
    primary_results = []
    did_you_mean_results = []
    seen = set()

    # Core prefix logic to prevent unrelated results (e.g., Wire Brushes for Aluminam)
    core_prefix = q_clean[:3]

    for hit in hits:
        source = hit.get("_source", {}) or {}
        score = hit.get("_score", 0)

        # Field weights for internal sorting
        fields = [
            ("attr", source.get("brand_category_product_type_attribute"), 3),
            ("type", source.get("brand_category_product_type"), 2),
            ("cat", source.get("brand_category"), 2),
            ("brand", source.get("brand_name"), 1),
        ]

        for f_type, values, priority in fields:
            if not values:
                continue
            if isinstance(values, str):
                values = [values]

            for v in values:
                v_val = v.strip()
                v_lower = v_val.lower()
                if v_val in seen:
                    continue

                # CHECK 1: Is it an exact word-for-word match? (Primary Bucket)
                # If 'aluminum' is in the text, it goes to primary.
                is_exact = all(word in v_lower for word in q_words)

                # CHECK 2: Relevance Filter
                # We allow fuzzy matches if it's a Brand (like Pferd)
                # OR if it contains the core prefix (Alu)
                is_brand = f_type == "brand"
                has_prefix = core_prefix in v_lower

                if is_exact:
                    primary_results.append(
                        {"text": v_val, "score": score, "priority": priority}
                    )
                    seen.add(v_val)
                elif score > 7.0 and (is_brand or has_prefix):
                    did_you_mean_results.append(
                        {"text": v_val, "score": score, "priority": priority}
                    )
                    seen.add(v_val)

    # Sort results: Attribute (3) > Type/Cat (2) > Brand (1)
    primary_results.sort(key=lambda x: (-x["priority"], -x["score"]))
    did_you_mean_results.sort(key=lambda x: (-x["priority"], -x["score"]))

    # ---------------------------------------------------------
    # 🔹 3. RESPONSE ASSEMBLY
    # ---------------------------------------------------------
    final_primary = [{"text": x["text"]} for x in primary_results[:size]]
    final_fallback = []
    fallback_type = None

    if final_primary:
        # If primary results exist but don't fill the size, fill with fuzzy/DYM
        if len(final_primary) < size:
            remaining = size - len(final_primary)
            final_primary.extend(
                [{"text": x["text"]} for x in did_you_mean_results[:remaining]]
            )

    elif did_you_mean_results:
        # No exact matches found, show fuzzy matches as Did You Mean
        final_fallback = [{"text": x["text"]} for x in did_you_mean_results[:size]]
        fallback_type = "did_you_mean"

    else:
        # Absolute Fallback: Popular Brands
        popular_query = {
            "size": 25,
            "_source": ["brand_name"],
            "query": {"match_all": {}},
        }
        try:
            pop_res = es.search(index=index_name, body=popular_query)
            pop_hits = pop_res.get("hits", {}).get("hits", [])
            seen_brands = set()
            for h in pop_hits:
                brand = (h["_source"].get("brand_name") or "").strip()
                if brand and brand not in seen_brands:
                    final_fallback.append({"text": brand})
                    seen_brands.add(brand)
                if len(final_fallback) >= 8:
                    break
            fallback_type = "top_brands"
        except:
            pass

    return {
        "success": True,
        "query": q,
        "primary_results": final_primary,
        "fallback_results": final_fallback,
        "fallback_type": fallback_type,
    }
