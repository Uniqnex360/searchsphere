from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, text
from sqlalchemy.orm import Session


class MPTTService:

    def __init__(self, model):
        self.model = model

    async def get_descendants(self, db: AsyncSession, node, include_self=False):
        """Get all descendants of a node"""
        if include_self:
            query = select(self.model).where(
                self.model.tree_id == node.tree_id,
                self.model.left >= node.left,
                self.model.right <= node.right,
            )
        else:
            query = select(self.model).where(
                self.model.tree_id == node.tree_id,
                self.model.left > node.left,
                self.model.right < node.right,
            )
        result = await db.exec(query)
        return result.all()

    async def insert_node(self, db: AsyncSession, node, parent=None):
        if parent is None:
            # New tree
            result = await db.exec(
                select(self.model.tree_id).order_by(self.model.tree_id.desc())
            )
            max_tree = result.first()
            node.tree_id = (max_tree or 0) + 1
            node.left = 1
            node.right = 2
            node.level = 0
        else:
            # Shift right values
            stmt_right = text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "right" = "right" + 2
                WHERE "right" >= :parent_right AND tree_id = :tree_id
                """
            )
            await db.exec(
                stmt_right.bindparams(parent_right=parent.right, tree_id=parent.tree_id)
            )

            # Shift left values
            stmt_left = text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "left" = "left" + 2
                WHERE "left" > :parent_right AND tree_id = :tree_id
                """
            )
            await db.exec(
                stmt_left.bindparams(parent_right=parent.right, tree_id=parent.tree_id)
            )

            node.tree_id = parent.tree_id
            node.level = parent.level + 1
            node.left = parent.right
            node.right = parent.right + 1
            node.parent_id = parent.id

        db.add(node)
        await db.commit()
        await db.refresh(node)
        return node

    async def get_ancestors(self, db: AsyncSession, node):
        """Get all ancestors of a node"""
        query = select(self.model).where(
            self.model.tree_id == node.tree_id,
            self.model.left < node.left,
            self.model.right > node.right,
        )
        result = await db.exec(query)
        return result.all()

    async def get_children(self, db: AsyncSession, node):
        """Get all children of a node"""
        query = select(self.model).where(self.model.parent_id == node.id)
        result = await db.exec(query)
        return result.all()

    async def get_siblings(self, db: AsyncSession, node):
        """Get siblings of a node"""
        query = select(self.model).where(
            self.model.parent_id == node.parent_id, self.model.id != node.id
        )
        result = await db.exec(query)
        return result.all()

    async def delete_node(self, db: AsyncSession, node):
        """Delete a node and its subtree"""
        width = node.right - node.left + 1

        # Delete subtree
        await db.exec(
            text(
                f"""
                DELETE FROM {self.model.__tablename__}
                WHERE "left" BETWEEN :left AND :right
                AND tree_id = :tree_id
                """
            ).bindparams(left=node.left, right=node.right, tree_id=node.tree_id),
        )

        # Shift remaining nodes
        await db.exec(
            text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "left" = "left" - :width
                WHERE "left" > :right AND tree_id = :tree_id
                """
            ).bindparams(width=width, right=node.right, tree_id=node.tree_id),
        )

        await db.exec(
            text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "right" = "right" - :width
                WHERE "right" > :right AND tree_id = :tree_id
                """
            ).bindparams(width=width, right=node.right, tree_id=node.tree_id),
        )

        await db.commit()

    @staticmethod
    def get_descendant_count(node):
        """Get the number of descendants of a node"""
        return (node.right - node.left - 1) // 2


class SyncMPTTService:

    def __init__(self, model):
        self.model = model

    def get_descendants(self, db: Session, node, include_self=False):
        """Get all descendants of a node"""
        if include_self:
            query = select(self.model).where(
                self.model.tree_id == node.tree_id,
                self.model.left >= node.left,
                self.model.right <= node.right,
            )
        else:
            query = select(self.model).where(
                self.model.tree_id == node.tree_id,
                self.model.left > node.left,
                self.model.right < node.right,
            )
        result = db.execute(query)
        return result.scalars().all()

    def insert_node(self, db: Session, node, parent=None):
        if parent is None:
            # New tree
            result = db.execute(
                select(self.model.tree_id).order_by(self.model.tree_id.desc())
            )
            max_tree = result.scalars().first()
            node.tree_id = (max_tree or 0) + 1
            node.left = 1
            node.right = 2
            node.level = 0
        else:
            # Shift right values
            stmt_right = text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "right" = "right" + 2
                WHERE "right" >= :parent_right AND tree_id = :tree_id
                """
            )
            db.execute(
                stmt_right.bindparams(parent_right=parent.right, tree_id=parent.tree_id)
            )

            # Shift left values
            stmt_left = text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "left" = "left" + 2
                WHERE "left" > :parent_right AND tree_id = :tree_id
                """
            )
            db.execute(
                stmt_left.bindparams(parent_right=parent.right, tree_id=parent.tree_id)
            )

            node.tree_id = parent.tree_id
            node.level = parent.level + 1
            node.left = parent.right
            node.right = parent.right + 1
            node.parent_id = parent.id

        db.add(node)
        db.commit()
        db.refresh(node)
        return node

    def get_ancestors(self, db: Session, node):
        """Get all ancestors of a node"""
        query = select(self.model).where(
            self.model.tree_id == node.tree_id,
            self.model.left < node.left,
            self.model.right > node.right,
        )
        result = db.execute(query)
        return result.scalars().all()

    def get_children(self, db: Session, node):
        """Get all children of a node"""
        query = select(self.model).where(self.model.parent_id == node.id)
        result = db.execute(query)
        return result.scalars().all()

    def get_siblings(self, db: Session, node):
        """Get siblings of a node"""
        query = select(self.model).where(
            self.model.parent_id == node.parent_id, self.model.id != node.id
        )
        result = db.execute(query)
        return result.scalars().all()

    def delete_node(self, db: Session, node):
        """Delete a node and its subtree"""
        width = node.right - node.left + 1

        # Delete subtree
        db.execute(
            text(
                f"""
                DELETE FROM {self.model.__tablename__}
                WHERE "left" BETWEEN :left AND :right
                AND tree_id = :tree_id
                """
            ).bindparams(left=node.left, right=node.right, tree_id=node.tree_id),
        )

        # Shift remaining nodes
        db.execute(
            text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "left" = "left" - :width
                WHERE "left" > :right AND tree_id = :tree_id
                """
            ).bindparams(width=width, right=node.right, tree_id=node.tree_id),
        )

        db.execute(
            text(
                f"""
                UPDATE {self.model.__tablename__}
                SET "right" = "right" - :width
                WHERE "right" > :right AND tree_id = :tree_id
                """
            ).bindparams(width=width, right=node.right, tree_id=node.tree_id),
        )

        db.commit()

    @staticmethod
    def get_descendant_count(node):
        """Get the number of descendants of a node"""
        return (node.right - node.left - 1) // 2
