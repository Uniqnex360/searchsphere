
from typing import List
from sqlmodel import Field, Relationship
from app.models import BaseModel


class Industry(BaseModel, table=True):

    __tablename__ = "industry"

    industry_name: str = Field(index=True, nullable=False, unique=True)

    # Relationships
    products: List["Product"] = Relationship(
        back_populates="industry", sa_relationship_kwargs={"lazy": "selectin"}
    )
    categories: List["Category"] = Relationship(back_populates="industry")
