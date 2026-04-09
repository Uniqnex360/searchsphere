import tempfile
from typing import Optional, List
from sqlalchemy import desc
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import APIRouter, File, UploadFile, HTTPException, Depends, Query

from app.models import APPImport
from app.database import get_session
from app.services import ImportType, CeleryTaskStatus


router = APIRouter()

UPLOAD_DIR = "/app/uploads"


@router.post("/import/product/")
async def upload_products_csv_v3(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
):

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".csv", dir=UPLOAD_DIR
    ) as tmp:
        tmp.write(await file.read())
        temp_path = tmp.name

    # ✅ 1. Create DB record (queued state)
    obj = APPImport(module_type=ImportType.PRODUCT)

    session.add(obj)
    await session.commit()
    await session.refresh(obj)

    # ✅ 2. Trigger celery
    from app.tasks import import_products_task

    task = import_products_task.delay(temp_path, obj.id)

    obj.task_id = task.id
    await session.commit()

    return {
        "message": "Import started",
        "task_id": task.id,
        "import_id": obj.id,
    }


@router.get("/import/list/")
async def get_import_list(
    session: AsyncSession = Depends(get_session),
    status: Optional[CeleryTaskStatus] = Query(None, description="Filter by status"),
    module_type: Optional[ImportType] = Query(
        None, description="Filter by module type"
    ),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Returns paginated list of APPImport entries with filters."""

    # Base query
    query = select(APPImport).order_by(desc(APPImport.created_at))

    if status:
        query = query.where(APPImport.status == status)
    if module_type:
        query = query.where(APPImport.module_type == module_type)

    # Count total rows
    total_result = await session.execute(select(APPImport).where(query._whereclause))
    total_count = len(total_result.scalars().all())

    # Fetch paginated results
    result = await session.execute(query.offset(offset).limit(limit))
    imports: List[APPImport] = result.scalars().all()

    data = [
        {
            "task_id": imp.task_id,
            "module_type": imp.module_type,
            "status": imp.status,
            "rows": imp.rows,
            "meta_data": imp.meta_data,
            "result": imp.result,
            "error": imp.error,
            "completed_at": imp.completed_at,
        }
        for imp in imports
    ]

    return {"total": total_count, "limit": limit, "offset": offset, "data": data}
