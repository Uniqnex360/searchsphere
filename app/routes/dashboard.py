from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, cast, String, and_
from elasticsearch import Elasticsearch

from app.database import get_session
from app.es_client import get_es
from app.models import ProductSearchResult, Product
from app.services import ESCollection

router = APIRouter()


# -----------------------------
# DATE RANGE HELPER
# -----------------------------
def get_day_range(date: datetime):
    start = datetime(date.year, date.month, date.day, 0, 0, 0)
    end = start + timedelta(days=1)
    return start, end


# -----------------------------
# DASHBOARD API (FIXED)
# -----------------------------
@router.get("/dashboard/product/search-keywords/")
async def product_search_dashboard(
    db: AsyncSession = Depends(get_session),
    es: Elasticsearch = Depends(get_es),
    start_date: datetime | None = Query(None),
    end_date: datetime | None = Query(None),
):

    # =========================================================
    # 1. SAFE BASE FILTER (IMPORTANT FIX)
    # =========================================================
    conditions = [ProductSearchResult.is_active == True]

    if start_date:
        start_date, _ = get_day_range(start_date)
        conditions.append(ProductSearchResult.created_at >= start_date)

    if end_date:
        _, end_date = get_day_range(end_date)
        conditions.append(ProductSearchResult.created_at < end_date)

    base_filter = and_(*conditions)

    # =========================================================
    # 2. NORMALIZED KEYWORD COLUMN
    # =========================================================
    keyword_col = func.lower(cast(ProductSearchResult.query["q"], String))

    # =========================================================
    # 3. RAW TOTAL SEARCHES (FIXED = ALWAYS MATCHES TABLE)
    # =========================================================
    total_searches = (await db.exec(select(func.count()).where(base_filter))).scalar()

    # =========================================================
    # 3.1 TOTAL PRODUCTS (NEW)
    # =========================================================
    query = select(func.count()).select_from(Product)

    if start_date:
        query = query.where(Product.created_at >= start_date)

    if end_date:
        query = query.where(Product.created_at < end_date)

    total_products = (await db.exec(query)).scalar()

    # =========================================================
    # 3.2 TOTAL ELASTICSEARCH DOCS (NEW)
    # =========================================================
    try:
        es_count_resp = es.count(index=ESCollection.PRODUCT_V7.value)
        total_es_docs = es_count_resp.get("count", 0)
    except Exception:
        total_es_docs = 0

    # =========================================================
    # 4. UNIQUE SEARCHES (GROUPED LOGIC)
    # =========================================================
    grouped_subquery = (
        select(
            keyword_col.label("q"),
            ProductSearchResult.total_result.label("total_result"),
        )
        .where(base_filter)
        .group_by(keyword_col, ProductSearchResult.total_result)
        .subquery()
    )

    unique_searches = (
        await db.exec(select(func.count()).select_from(grouped_subquery))
    ).scalar()

    # =========================================================
    # 5. SUCCESS / FAILURE METRICS (RAW)
    # =========================================================
    is_success = case((ProductSearchResult.total_result > 0, 1), else_=0)

    is_zero = case((ProductSearchResult.total_result == 0, 1), else_=0)

    metrics = (
        await db.exec(
            select(
                func.sum(is_zero).label("zero_result_searches"),
                func.sum(is_success).label("successful_searches"),
                func.avg(ProductSearchResult.total_result).label("avg_results"),
            ).where(base_filter)
        )
    ).one()

    zero_result_searches = metrics.zero_result_searches or 0
    successful_searches = metrics.successful_searches or 0
    avg_results = metrics.avg_results or 0

    # =========================================================
    # 6. UNIQUE SUCCESS / FAILURE KEYWORDS
    # =========================================================
    success_unique_query = (
        select(keyword_col, ProductSearchResult.total_result)
        .where(base_filter)
        .where(ProductSearchResult.total_result > 0)
        .group_by(keyword_col, ProductSearchResult.total_result)
    )

    failed_unique_query = (
        select(keyword_col, ProductSearchResult.total_result)
        .where(base_filter)
        .where(ProductSearchResult.total_result == 0)
        .group_by(keyword_col, ProductSearchResult.total_result)
    )

    unique_successful_keywords = (
        await db.exec(select(func.count()).select_from(success_unique_query.subquery()))
    ).scalar()

    unique_failed_keywords = (
        await db.exec(select(func.count()).select_from(failed_unique_query.subquery()))
    ).scalar()

    # =========================================================
    # 7. FAILURE RATE
    # =========================================================
    failure_rate = (
        (zero_result_searches / total_searches * 100) if total_searches else 0
    )

    # =========================================================
    # 8. TOP KEYWORD
    # =========================================================
    top_keyword = (
        await db.exec(
            select(keyword_col.label("q"), func.count().label("count"))
            .where(base_filter)
            .group_by(keyword_col)
            .order_by(func.count().desc())
            .limit(1)
        )
    ).first()

    # =========================================================
    # FINAL RESPONSE
    # =========================================================
    return {
        "total_searches": total_searches,
        "unique_searches": unique_searches,
        "zero_result_searches": zero_result_searches,
        "successful_searches": successful_searches,
        "avg_results_per_search": float(avg_results),
        "failure_rate_percent": round(failure_rate, 2),
        "unique_successful_keywords": unique_successful_keywords,
        "unique_failed_keywords": unique_failed_keywords,
        "top_keyword": {
            "q": top_keyword.q if top_keyword else None,
            "count": top_keyword.count if top_keyword else 0,
        },
        # ✅ NEW FIELDS ADDED (ONLY THESE TWO)
        "total_products": total_products,
        "total_es_docs": total_es_docs,
    }
