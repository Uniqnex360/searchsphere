from pydantic import BaseModel
from fastapi import APIRouter, Depends
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.services import CategoryService
from app.models import Category

router = APIRouter()


class PathCreateRequest(BaseModel):
    industry_name: str
    path: str


@router.post("/categories/from-path")
async def create_category_from_path(
    payload: PathCreateRequest,
    db: AsyncSession = Depends(get_session),
):
    service = CategoryService(Category)

    node = await service.create_from_path(
        db,
        industry_name=payload.industry_name,
        path=payload.path,
    )

    return node


@router.get("/categories/test")
async def test_category_method(db: AsyncSession = Depends(get_session)):
    service = CategoryService(Category)

    industry_id = await service.get_industry_id(db, "Electronics")

    elec = await service.get_or_create_node(
        db,
        name="Electronics",
        industry_id=industry_id,
    )

    phone = await service.get_or_create_node(
        db,
        name="Phone",
        industry_id=industry_id,
        parent=elec,
    )

    result = await service.delete_node(db, phone)
    return {"status": "deleted"}
