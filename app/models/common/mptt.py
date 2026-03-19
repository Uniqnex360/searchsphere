from sqlmodel import Field
from app.models import BaseModel


class MPTTBase(BaseModel):
    """
    base mptt model to store hierarchical data
    Data structure used -> modified PreOrder Tree Traversal
    """

    left: int = Field(index=True)
    right: int = Field(index=True)
    level: int = Field(index=True)
    tree_id: int = Field(index=True)


