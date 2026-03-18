from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class BaseModel(SQLModel):
    """Common base fields."""

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)
    is_active: bool = Field(default=True, nullable=False)
    is_deleted: bool = Field(default=False, nullable=False)


class BaseURLModel(BaseModel):
    """Base for URL tables (images, documents, videos)."""

    name: Optional[str] = Field(default=None)
    url: str = Field(...)  # required column
