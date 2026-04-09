from sqlmodel import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy import null

from app.models import Industry
from app.services import MPTTService, SyncMPTTService


class CategoryService(MPTTService):

    def __init__(self, model):
        super().__init__(model)
        self._cache = {}

    async def get_industry_id(self, db, industry_name: str) -> int:
        result = await db.exec(
            select(Industry).where(Industry.industry_name == industry_name)
        )
        industry = result.first()

        if not industry:
            raise ValueError(f"Industry '{industry_name}' not found")

        return industry.id

    async def get_or_create_node(self, db, *, name: str, industry_id: int, parent=None):
        # ✅ Normalize (optional but recommended)
        name = name.strip()

        parent_id = parent.id if parent else None

        # ✅ Cache key
        cache_key = f"{industry_id}-{parent_id}-{name}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        # ✅ Build query
        query = select(self.model).where(
            self.model.name == name,
            self.model.industry_id == industry_id,
        )

        if parent:
            query = query.where(self.model.parent_id == parent.id)
        else:
            query = query.where(self.model.parent_id.is_(None))

        result = await db.exec(query)
        node = result.first()

        if node:
            self._cache[cache_key] = node
            return node

        # ✅ Create new node
        new_node = self.model(
            name=name,
            industry_id=industry_id,
        )

        try:
            node = await self.insert_node(db, new_node, parent)
            self._cache[cache_key] = node
            return node

        except IntegrityError:
            # 🔥 Race condition fix (another request inserted same row)
            await db.rollback()

            result = await db.exec(query)
            node = result.first()

            if node:
                self._cache[cache_key] = node
                return node

            # If still not found → raise (something else went wrong)
            raise

    async def create_from_path(self, db, *, industry_name: str, path: str):
        parts = [p.strip() for p in path.split(">") if p.strip()]
        if not parts:
            raise ValueError("Invalid path")

        industry_id = await self.get_industry_id(db, industry_name)

        parent = None
        last_node = None

        for level_name in parts:
            node = await self.get_or_create_node(
                db=db,
                name=level_name,
                industry_id=industry_id,
                parent=parent,
            )
            parent = node
            last_node = node

        return last_node


class SyncCategoryService(SyncMPTTService):

    def __init__(self, model):
        super().__init__(model)
        self._cache = {}

    def get_industry_id(self, db, industry_name: str) -> int:
        result = db.execute(
            select(Industry).where(Industry.industry_name == industry_name)
        )
        industry = result.scalars().first()

        if not industry:
            raise ValueError(f"Industry '{industry_name}' not found")

        return industry.id

    def get_or_create_node(self, db, *, name: str, industry_id: int, parent=None):
        # ✅ Normalize (optional but recommended)
        name = name.strip()

        parent_id = parent.id if parent else None

        # ✅ Cache key
        cache_key = f"{industry_id}-{parent_id}-{name}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        # ✅ Build query
        query = select(self.model).where(
            self.model.name == name,
            self.model.industry_id == industry_id,
        )

        if parent:
            query = query.where(self.model.parent_id == parent.id)
        else:
            query = query.where(self.model.parent_id.is_(None))

        result = db.execute(query)
        node = result.scalars().first()

        if node:
            self._cache[cache_key] = node
            return node

        # ✅ Create new node
        new_node = self.model(
            name=name,
            industry_id=industry_id,
        )

        try:
            node = self.insert_node(db, new_node, parent)
            self._cache[cache_key] = node
            return node

        except IntegrityError:
            # 🔥 Race condition fix (another request inserted same row)
            db.rollback()

            result = db.execute(query)
            node = result.scalars().first()

            if node:
                self._cache[cache_key] = node
                return node

            # If still not found → raise (something else went wrong)
            raise

    def create_from_path(self, db, *, industry_name: str, path: str):
        parts = [p.strip() for p in path.split(">") if p.strip()]
        if not parts:
            raise ValueError("Invalid path")

        industry_id = self.get_industry_id(db, industry_name)

        parent = None
        last_node = None

        for level_name in parts:
            node = self.get_or_create_node(
                db=db,
                name=level_name,
                industry_id=industry_id,
                parent=parent,
            )
            parent = node
            last_node = node

        return last_node
