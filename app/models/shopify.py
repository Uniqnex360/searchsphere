from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime

from app.models import BaseModel


class ShopifyAuth(BaseModel, table=True):

    shop: str = Field(index=True, unique=True, nullable=False)
    access_token: str = Field(nullable=False)

    scope: Optional[str] = None

    is_active: bool = Field(default=True)

    installed_at: datetime = Field(default_factory=datetime.utcnow)