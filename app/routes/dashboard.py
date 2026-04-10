from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, cast, String, and_

from app.database import get_session
from app.models import ProductSearchResult

router = APIRouter()


# -----------------------------
# CONVERT DATE → FULL DAY RANGE
# -----------------------------
def get_day_range(date: datetime):
    start = datetime(date.year, date.month, date.day, 0, 0, 0)
    end = start + timedelta(days=1)
    return start, end


@router.get("/dashboard/product/search-keywords/")
async def product_search_dashboard(
    db: AsyncSession = Depends(get_session),
    start_date: datetime | None = Query(None),
    end_date: datetime | None = Query(None),
):
    # -----------------------------
    # BASE CONDITIONS
    # -----------------------------
    base_conditions = [ProductSearchResult.is_active == True]

    # convert DATE → FULL DAY RANGE
    if start_date:
        start_date, _ = get_day_range(start_date)
        base_conditions.append(ProductSearchResult.created_at >= start_date)

    if end_date:
        _, end_date = get_day_range(end_date)
        base_conditions.append(ProductSearchResult.created_at < end_date)

    base_filter = and_(*base_conditions)

    # -----------------------------
    # KEY FIELD
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
        "start_date": start_date,
        "end_date": end_date,
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
