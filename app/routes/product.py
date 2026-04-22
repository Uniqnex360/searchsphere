import re
import time
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func, desc
from sqlalchemy.orm import selectinload, joinedload
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
    brand_: Optional[List[str]] = Query(
        None, alias="brand"
    ),  # Updated to match frontend params
    product_type_: Optional[List[str]] = Query(None, alias="product_type"),
    category_: Optional[List[str]] = Query(None, alias="category"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    es: Elasticsearch = Depends(get_es),
    session: AsyncSession = Depends(get_session),
    sort_by: str | None = None,
    sort_order: str = "desc",
    page: int = 1,
):
    start_total = time.perf_counter()

    # 1. Extract Dynamic Attribute Filters
    start_attr = time.perf_counter()
    attr_filters: Dict[str, List[str]] = {}
    for key, value in request.query_params.multi_items():
        if key.startswith("attr_"):
            attr_name = key.replace("attr_", "")
            # Split by comma because frontend sends: attr_Color=Red,Blue
            attr_values = value.split(",")
            if attr_name in attr_filters:
                attr_filters[attr_name].extend(attr_values)
            else:
                attr_filters[attr_name] = attr_values

    attr_duration = time.perf_counter() - start_attr
    print(f"Timing - Dynamic Attr Extraction: {attr_duration:.4f}s")

    # 2. Prepare Standard Filters
    filters = {
        "brand": brand_,
        "product_type": product_type_,
        "category": category_,
        "price_min": price_min,
        "price_max": price_max,
        "attr_filters": attr_filters,  # Added to the payload for logging
    }

    # 3. Call your ES logic
    start_es = time.perf_counter()
    data = await get_product_list_v6(
        es,
        q,
        brand=brand_,
        product_type=product_type_,
        category=category_,
        attr_filters=attr_filters,  # <--- PASSING THE DYNAMIC DICT HERE
        min_price=price_min,
        max_price=price_max,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
    )
    es_duration = time.perf_counter() - start_es
    print(f"Timing - Elasticsearch Query: {es_duration:.4f}s")

    query_payload = {
        "q": q,
        "processed_query": data.get("processed_query"),
        "filters": filters,
        "page": page,
    }

    # 4. Background Tasks
    background_tasks.add_task(
        save_search_result,
        es,
        session,
        query_payload,
        data,
        request.headers.get("X-FE-URL", str(request.url)),
    )

    if q:
        product_ids = [item["id"] for item in data.get("results", [])]
        background_tasks.add_task(
            increment_search_popularity, es, ESCollection.PRODUCT_V7.value, product_ids
        )

    total_duration = time.perf_counter() - start_total
    print(f"Timing - Total Request v6: {total_duration:.4f}s")

    return {"total": data.get("total_docs_after_filter", 0), "data": data}


# -----------------------------
# 🔥 CLEAN FUNCTION (remove junk)
# -----------------------------
def clean_text(text: str):
    if not text:
        return None

    text = str(text).strip().lower()

    # normalize rpm spacing
    text = re.sub(r"\s*rpm", "rpm", text)

    # remove pure numbers / garbage
    if re.match(r"^[\d\-\s]+$", text):
        return None

    if len(text) < 3:
        return None

    return text


@router.get("/product/v6/auto-complete/")
async def get_product_auto_complete_v6(
    q: str = Query(..., min_length=1),
    size: int = 10,
    es: Elasticsearch = Depends(get_es),
):
    index_name = ESCollection.PRODUCT_AUTO_SUGGEST_V7.value
    product_index = ESCollection.PRODUCT_V7.value

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
    # 🔹 1. MAIN SEARCH QUERY (UNCHANGED)
    # ---------------------------------------------------------
    search_query = {
        "size": 200,
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": q_clean,
                            "type": "bool_prefix",
                            "fields": [
                                "brand_category_product_type_attribute.autocomplete^25",
                                "brand_category_product_type.autocomplete^15",
                                "brand_category.autocomplete^10",
                                "brand_name.autocomplete^5",
                            ],
                            "boost": 10,
                        }
                    },
                    {
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
    # 🔹 2. BUCKETING
    # ---------------------------------------------------------
    primary_results = []
    did_you_mean_results = []
    seen = set()

    core_prefix = q_clean[:3]

    for hit in hits:
        source = hit.get("_source", {}) or {}
        score = hit.get("_score", 0)

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
                v_clean = clean_text(v)
                if not v_clean or v_clean in seen:
                    continue

                is_exact = all(word in v_clean for word in q_words)
                is_brand = f_type == "brand"
                has_prefix = core_prefix in v_clean

                if is_exact:
                    primary_results.append(
                        {"text": v_clean, "score": score, "priority": priority}
                    )
                    seen.add(v_clean)

                elif score > 7.0 and (is_brand or has_prefix):
                    did_you_mean_results.append(
                        {"text": v_clean, "score": score, "priority": priority}
                    )
                    seen.add(v_clean)

    primary_results.sort(key=lambda x: (-x["priority"], -x["score"]))
    did_you_mean_results.sort(key=lambda x: (-x["priority"], -x["score"]))

    final_primary = [{"text": x["text"]} for x in primary_results[:size]]
    final_fallback = []
    fallback_type = None

    # ---------------------------------------------------------
    # 🔥 STEP 1: PRIMARY EXISTS → ATTRIBUTE → PRODUCT NAME
    # ---------------------------------------------------------
    if final_primary:
        if len(final_primary) < size:
            attr_query = {
                "size": 50,
                "_source": ["product_name", "attributes.value"],
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "attributes.value": {
                                        "query": q_clean,
                                        "operator": "and",
                                    }
                                }
                            },
                            {
                                "match": {
                                    "product_name": {
                                        "query": q_clean,
                                        "fuzziness": "AUTO",
                                        "boost": 3,
                                    }
                                }
                            },
                        ]
                    }
                },
            }

            try:
                attr_res = es.search(index=product_index, body=attr_query)
                attr_hits = attr_res.get("hits", {}).get("hits", [])

                seen_final = set([x["text"] for x in final_primary])

                for h in attr_hits:
                    src = h.get("_source", {}) or {}

                    # 🔥 ONLY PRODUCT NAME (NO ATTRIBUTES SHOWN)
                    pname = clean_text(src.get("product_name"))
                    if pname and pname not in seen_final:
                        final_primary.append({"text": pname})
                        seen_final.add(pname)

                    if len(final_primary) >= size:
                        break

            except:
                pass

    # ---------------------------------------------------------
    # 🔥 STEP 2: NO PRIMARY → ATTRIBUTE FIRST
    # ---------------------------------------------------------
    else:
        attr_query = {
            "size": 50,
            "_source": ["product_name", "attributes.value"],
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "attributes.value": {
                                    "query": q_clean,
                                    "operator": "and",
                                }
                            }
                        },
                        {
                            "match": {
                                "product_name": {
                                    "query": q_clean,
                                    "fuzziness": "AUTO",
                                }
                            }
                        },
                    ]
                }
            },
        }

        try:
            attr_res = es.search(index=product_index, body=attr_query)
            attr_hits = attr_res.get("hits", {}).get("hits", [])

            seen_final = set()

            for h in attr_hits:
                src = h.get("_source", {}) or {}

                pname = clean_text(src.get("product_name"))
                if pname and pname not in seen_final:
                    final_fallback.append({"text": pname})
                    seen_final.add(pname)

                if len(final_fallback) >= size:
                    break

        except:
            pass

        # ---------------------------------------------------------
        # 🔥 STEP 3: DID YOU MEAN
        # ---------------------------------------------------------
        if len(final_fallback) < size and did_you_mean_results:
            remaining = size - len(final_fallback)
            final_fallback.extend(
                [{"text": x["text"]} for x in did_you_mean_results[:remaining]]
            )
            fallback_type = "attribute_then_did_you_mean"

        # ---------------------------------------------------------
        # 🔥 STEP 4: BRANDS
        # ---------------------------------------------------------
        if len(final_fallback) < size:
            popular_query = {
                "size": 25,
                "_source": ["brand_name"],
                "query": {"match_all": {}},
            }

            try:
                pop_res = es.search(index=index_name, body=popular_query)
                pop_hits = pop_res.get("hits", {}).get("hits", [])

                seen_brands = set([x["text"] for x in final_fallback])

                for h in pop_hits:
                    brand = (h["_source"].get("brand_name") or "").strip()
                    if brand and brand not in seen_brands:
                        final_fallback.append({"text": brand})
                        seen_brands.add(brand)

                    if len(final_fallback) >= size:
                        break
            except:
                pass

            fallback_type = "final_brand_fallback"

    return {
        "success": True,
        "query": q,
        "primary_results": final_primary,
        "fallback_results": final_fallback,
        "fallback_type": fallback_type,
    }


from itertools import permutations


def build_suggestions_chain(brand=None, product_type=None, category=None):
    """
    Build autocomplete suggestions using permutations of brand, product_type, category.
    Returns a list of unique suggestions.
    """
    parts = [p for p in [brand, product_type, category] if p]

    # Generate all non-empty permutations
    suggestions = set()
    for r in range(1, len(parts) + 1):
        for perm in permutations(parts, r):
            suggestions.add(" ".join(perm))

    return list(suggestions)


@router.get("/product/sync-missing-es/")
async def sync_missing_products_to_es(
    sync: bool = Query(
        False, description="If True, actually index and delete products"
    ),
    batch_size: int = Query(300, le=1000),
    index_name: str = ESCollection.PRODUCT_V7.value,
    session: AsyncSession = Depends(get_session),
    es: Elasticsearch = Depends(get_es),
):
    es_service = ElasticsearchService(es, index_name)
    last_id = 0
    missing_count = 0
    deleted_count = 0
    synced_count = 0
    all_missing_metadata = []
    all_errors = []

    print(f"🚀 Starting Two-Way Sync. Mode: {sync} | Index: {index_name}")

    # --- STEP 1: FORWARD SYNC (Add Missing from PG to ES) ---
    while True:
        stmt = (
            select(Product)
            .options(
                selectinload(Product.images),
                selectinload(Product.features),
                selectinload(Product.attributes),
                selectinload(Product.category),
                selectinload(Product.brand),
                selectinload(Product.product_type),
            )
            .where(Product.id > last_id)
            .where(getattr(Product, "deleted_at", None) == None)
            .order_by(Product.id)
            .limit(batch_size)
        )

        result = await session.execute(stmt)
        products = result.scalars().all()
        if not products:
            break

        product_map = {str(p.id): p for p in products}
        batch_ids = list(product_map.keys())

        # Check ES existence
        es_res = es.search(
            index=index_name,
            query={"ids": {"values": batch_ids}},
            source=False,
            size=len(batch_ids),
        )
        found_ids = {hit["_id"] for hit in es_res["hits"]["hits"]}

        missing_batch_products = [
            product_map[pid] for pid in batch_ids if pid not in found_ids
        ]

        if missing_batch_products:
            actions = []
            for product in missing_batch_products:
                # --- Transformation Logic ---
                brand_name = product.brand.brand_name if product.brand else None
                cat_name = product.category.name if product.category else None
                type_name = (
                    product.product_type.product_type if product.product_type else None
                )

                all_suggestions = build_suggestions_chain(
                    brand=brand_name, product_type=type_name, category=cat_name
                )
                all_suggestions += [v for v in [product.mpn, product.sku] if v]
                all_suggestions = list(dict.fromkeys(all_suggestions))
                all_suggestions.append(product.product_name)

                data = {
                    "product_name": product.product_name,
                    "sku": product.sku,
                    "mpn": product.mpn,
                    "brand": brand_name,
                    "category": cat_name,
                    "all_suggestions": all_suggestions,
                    "features": [
                        {"name": f.name, "value": f.value} for f in product.features
                    ],
                    "attributes": [
                        {
                            "name": a.attribute_name,
                            "value": a.attribute_value,
                            "uom": a.attribute_uom,
                        }
                        for a in product.attributes
                    ],
                    "images": [
                        {"name": img.name, "url": img.url} for img in product.images
                    ],
                }

                actions.append(
                    {
                        "_index": index_name,
                        "_op_type": "index",
                        "_id": str(product.id),
                        "_source": data,
                    }
                )

                all_missing_metadata.append(
                    {"id": product.id, "name": product.product_name}
                )

            if sync and actions:
                success, errors = es_service.bulk(actions=actions)
                synced_count += success
                if errors:
                    all_errors.extend(errors[:5])

            missing_count += len(missing_batch_products)

        last_id = int(batch_ids[-1])
        print(
            f"✅ Forward Sync: Checked up to ID {last_id}. Missing found so far: {missing_count}"
        )

    # --- STEP 2: BACKWARD SYNC (Remove Orphans from ES) ---
    print("🔍 Checking for extra (orphan) docs in Elasticsearch...")

    # Fetching IDs from ES. For more than 10k docs, use helpers.scan instead.
    scroll_res = es.search(
        index=index_name,
        query={"match_all": {}},
        source=False,
        fields=["_id"],
        size=10000,
    )

    es_all_ids = [hit["_id"] for hit in scroll_res["hits"]["hits"]]

    # Process in smaller chunks to avoid Postgres parameter limits and type errors
    sql_chunk_size = 500
    for i in range(0, len(es_all_ids), sql_chunk_size):
        chunk = es_all_ids[i : i + sql_chunk_size]

        # FIX: Convert ES string IDs to Integers for Postgres compatibility
        try:
            int_chunk = [int(oid) for oid in chunk]
        except ValueError:
            # Handle cases where ES IDs might not be numeric
            continue

        # Check which IDs actually exist in Postgres
        valid_pg_stmt = select(Product.id).where(Product.id.in_(int_chunk))
        valid_pg_res = await session.execute(valid_pg_stmt)
        # Convert back to strings for comparison with ES IDs
        valid_pg_ids = {str(r) for r in valid_pg_res.scalars().all()}

        # Find IDs that are in ES but NOT in the valid Postgres list
        orphans = [oid for oid in chunk if oid not in valid_pg_ids]

        if orphans:
            if sync:
                delete_actions = [
                    {"_op_type": "delete", "_index": index_name, "_id": oid}
                    for oid in orphans
                ]
                success, d_errors = es_service.bulk(actions=delete_actions)
                deleted_count += len(delete_actions)
                if d_errors:
                    all_errors.extend(d_errors[:5])
            else:
                deleted_count += len(orphans)

    print(f"🧹 Orphan check complete. Orphans found: {deleted_count}")

    return {
        "status": "Success" if not all_errors else "Partial Success",
        "sync_enabled": sync,
        "missing_created": synced_count,
        "orphans_deleted": deleted_count,
        "total_missing_detected": missing_count,
        "sample_data": all_missing_metadata[:10],
        "errors": all_errors[:5],
    }
