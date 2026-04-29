from typing import Optional, Dict, Any
from datetime import datetime
from sqlmodel import Field
from sqlalchemy import Column, JSON

from app.models import BaseModel
from app.services import CeleryTaskStatus, ImportType


class APPImport(BaseModel, table=True):
    """Model that tracks bulk import details"""

    task_id: Optional[str] = Field(default=None, index=True)
    module_type: ImportType = Field(default=ImportType.PRODUCT)
    file_name: Optional[str] = Field(default=None)
    status: CeleryTaskStatus = Field(default=CeleryTaskStatus.PENDING)

    rows: Optional[int] = Field(default=None)

    meta_data: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    result: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))

    error: Optional[str] = Field(default=None)

    completed_at: Optional[datetime] = Field(default=None)
