from typing import Type, Tuple
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy import select


async def get_or_create(
    db: AsyncSession, model: Type[SQLModel], defaults: dict = None, **kwargs
) -> Tuple[SQLModel, bool]:
    """
    Get an existing row by filter kwargs, or create it if not found.

    Args:
        db: AsyncSession
        model: SQLModel class
        defaults: dict of fields to set if creating
        **kwargs: filter fields to look for existing row

    Returns:
        Tuple[instance, created]
            - instance: SQLModel object
            - created: True if created, False if existing
    """
    defaults = defaults or {}

    stmt = select(model).filter_by(**kwargs)
    result = await db.exec(stmt)
    instance = result.scalars().first()

    if instance:
        return instance, False

    # Not found → create
    params = {**kwargs, **defaults}
    instance = model(**params)
    db.add(instance)
    await db.commit()
    await db.refresh(instance)
    return instance, True
