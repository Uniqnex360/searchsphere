from sqlmodel import Field, Relationship
from typing import Optional, List
from datetime import datetime

from app.models import BaseModel


class ShopifyAuth(BaseModel, table=True):

    shop: str = Field(index=True, unique=True, nullable=False)
    access_token: str = Field(nullable=False)

    scope: Optional[str] = None

    is_active: bool = Field(default=True)

    installed_at: datetime = Field(default_factory=datetime.utcnow)

    products: List["Product"] = Relationship(
        back_populates="shopify_auth", sa_relationship_kwargs={"lazy": "selectin"}
    )