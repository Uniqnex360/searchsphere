from typing import Optional, List
from elasticsearch import Elasticsearch
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func, desc
from sqlalchemy.orm import selectinload

from fastapi import APIRouter, Depends, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from app.models import Product, Category, ProductSearchResult
from app.services import (
    ESCollection,
    autocomplete_products,
    autocomplete_product_vector,
    autocomplete_with_es_qdrant,
    get_product_auto_complete_v3,
    get_product_auto_complete_v4,
    create_product_mapping,
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
    brand_stmt = select(Product.brand).distinct().order_by(Product.brand.asc())
    brand_result = await db.execute(brand_stmt)
    brands = [row[0] for row in brand_result.fetchall()]

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
    data = await get_product_auto_complete_v4(
        es,
        q,
        brand=filters.get("brand"),
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


@router.get("/product/search/keywords/")
async def get_product_search_keywords_list(
    request: Request,
    db: AsyncSession = Depends(get_session),
):
    page, limit = get_pagination_params(request)
    offset = (page - 1) * limit

    # ✅ total count
    total_result = await db.exec(select(func.count()).select_from(ProductSearchResult))
    total = total_result.one()  # ✅ FIXED

    # ✅ main query
    result = await db.exec(
        select(ProductSearchResult)
        .order_by(ProductSearchResult.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    data = result.all()  # ✅ IMPORTANT: no scalars() for SQLModel

    return {
        "data": data,
        "meta": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit,
        },
    }
