from typing import Optional, List
from elasticsearch import Elasticsearch
from fastapi import APIRouter, Depends, Query
from app.services import (
    autocomplete_products,
    autocomplete_product_vector,
    autocomplete_with_es_qdrant,
)
from app.es_client import get_es
from app.services import get_qdrant_client

router = APIRouter()


@router.get("/product/auto-complete/")
async def autocomplate_product(q: str, es: Elasticsearch = Depends(get_es)):
    data = await autocomplete_products(es, q)
    return {"total": 0, "data": data}


@router.get("/product/vector/auto-complete/")
async def autocomplate_product_vector(
    q: str,
    brand: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    price_min: Optional[int] = Query(None),
    price_max: Optional[int] = Query(None),
    es: Elasticsearch = Depends(get_es),
    qdrant: Elasticsearch = Depends(get_qdrant_client),
    sort_by: str | None = None,
):

    filters = {
        "brand": brand,
        "category": category,
        "price_min": price_min,
        "price_max": price_max,
    }
    data = await autocomplete_with_es_qdrant(
        es, qdrant, q, filters=filters, sort_by=sort_by
    )
    return {"total": len(data), "data": data}
