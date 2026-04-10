from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, cast, String, and_

from app.database import get_session
from app.models import ProductSearchResult

router = APIRouter()


@router.get("/dashboard/product/search-keywords/")
async def product_search_dashboard(
    db: AsyncSession = Depends(get_session),
    range: str = Query("all", enum=["all", "day", "week", "month"]),
):
    now = datetime.utcnow()

    # -----------------------------
    # DATE RANGE
    # -----------------------------
    start_date = None

    if range == "day":
        start_date = now - timedelta(days=1)
    elif range == "week":
        start_date = now - timedelta(days=7)
    elif range == "month":
        start_date = now - timedelta(days=30)

    # -----------------------------
    # FIXED FILTER LOGIC (IMPORTANT)
    # -----------------------------
    base_conditions = [ProductSearchResult.is_active == True]

    if start_date is not None:
        base_conditions.append(ProductSearchResult.created_at >= start_date)

    base_filter = and_(*base_conditions)

    # -----------------------------
    # KEY FIELDS
    # -----------------------------
    keyword_col = func.lower(cast(ProductSearchResult.query["q"], String))

    is_success = case((ProductSearchResult.total_result > 0, 1), else_=0)
    is_zero = case((ProductSearchResult.total_result == 0, 1), else_=0)

    # -----------------------------
    # MAIN QUERY
    # -----------------------------
    query = select(
        func.count().label("total_searches"),
        func.count(func.distinct(keyword_col)).label("unique_searches"),
        func.sum(is_zero).label("zero_result_searches"),
        func.sum(is_success).label("successful_searches"),
        func.avg(ProductSearchResult.total_result).label("avg_results"),
        func.count(
            func.distinct(case((ProductSearchResult.total_result > 0, keyword_col)))
        ).label("unique_successful_keywords"),
        func.count(
            func.distinct(case((ProductSearchResult.total_result == 0, keyword_col)))
        ).label("unique_failed_keywords"),
    ).where(base_filter)

    result = await db.exec(query)
    row = result.one()

    # -----------------------------
    # FAILURE RATE
    # -----------------------------
    failure_rate = (
        (row.zero_result_searches / row.total_searches * 100)
        if row.total_searches
        else 0
    )

    # -----------------------------
    # TOP KEYWORD
    # -----------------------------
    top_keyword_query = (
        select(keyword_col.label("q"), func.count().label("count"))
        .where(base_filter)
        .group_by(keyword_col)
        .order_by(func.count().desc())
        .limit(1)
    )

    top_keyword_result = await db.exec(top_keyword_query)
    top_keyword = top_keyword_result.first()

    # -----------------------------
    # RESPONSE
    # -----------------------------
    return {
        "range": range,
        "total_searches": row.total_searches,
        "unique_searches": row.unique_searches,
        "zero_result_searches": row.zero_result_searches,
        "successful_searches": row.successful_searches,
        "avg_results_per_search": float(row.avg_results or 0),
        "failure_rate_percent": round(failure_rate, 2),
        "unique_successful_keywords": row.unique_successful_keywords,
        "unique_failed_keywords": row.unique_failed_keywords,
        "top_keyword": {
            "q": top_keyword.q if top_keyword else None,
            "count": top_keyword.count if top_keyword else 0,
        },
    }
