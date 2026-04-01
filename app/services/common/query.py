from typing import Type, Tuple
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy import select


async def get_or_create(
    db: AsyncSession, model: Type[SQLModel], defaults: dict = None, **kwargs
) -> Tuple[SQLModel, bool]:

    defaults = defaults or {}

    stmt = select(model).filter_by(**kwargs)
    result = await db.exec(stmt)
    instance = result.scalars().first()

    if instance:
        return instance, False

    # create new
    params = {**kwargs, **defaults}
    instance = model(**params)

    db.add(instance)
    await db.flush()

    return instance, True
