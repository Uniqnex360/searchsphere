from typing import Optional, List
from elasticsearch import Elasticsearch
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from fastapi import APIRouter, Depends, Query
from app.models import Product, Category
from app.services import (
    autocomplete_products,
    autocomplete_product_vector,
    autocomplete_with_es_qdrant,
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
