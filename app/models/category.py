from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlmodel import Field, Relationship
from typing import Optional, List
from app.models import MPTTBase


class Category(MPTTBase, table=True):
    __tablename__ = "category"

    __table_args__ = (
        UniqueConstraint(
            "industry_id",
            "parent_id",
            "name",
            name="uq_category_per_parent",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

    parent_id: Optional[int] = Field(default=None, foreign_key="category.id")

    industry_id: Optional[int] = Field(
        default=None, sa_column=Column(ForeignKey("industry.id"), nullable=True)
    )

    industry: Optional["Industry"] = Relationship(back_populates="categories")
    products: List["Product"] = Relationship(back_populates="category")
