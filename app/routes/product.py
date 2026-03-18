from elasticsearch import Elasticsearch
from fastapi import APIRouter, Query, Depends
from app.services import autocomplete_products
from app.es_client import get_es

router = APIRouter()


@router.get("/product/auto-complete/")
async def autocomplate_product(q: str, es: Elasticsearch = Depends(get_es)):
    data = await autocomplete_products(es, q)
    return {"total": 0, "data": data}
