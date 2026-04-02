from typing import List
from sqlmodel import Field, Relationship
from app.models import BaseModel


class Brand(BaseModel, table=True):
    """brand table for entire app"""

    __tablename__ = "brand"

    brand_name: str = Field(index=True, nullable=False, unique=True)

    # Relationships
    products: List["Product"] = Relationship(
        back_populates="brand", sa_relationship_kwargs={"lazy": "selectin"}
    )
