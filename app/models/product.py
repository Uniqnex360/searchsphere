from sqlmodel import Field, Relationship, Column
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON
from typing import List, Optional, Dict, Any

from app.models import BaseModel, BaseURLModel


class Product(BaseModel, table=True):
    """base model for Product"""

    __tablename__ = "product"

    __table_args__ = (UniqueConstraint("mpn", name="uq_product_mpn"),)

    # ✅ SKU can be NULL now
    sku: Optional[str] = Field(default=None, index=True, nullable=True)

    product_name: str

    # ✅ MPN is now the unique identifier
    mpn: Optional[str] = Field(default=None, index=True)

    brand: Optional[str] = None
    gtin: Optional[str] = None
    ean: Optional[str] = None
    upc: Optional[str] = None
    taxonomy: Optional[str] = None
    country_of_origin: Optional[str] = None
    warranty: Optional[str] = None

    weight: Optional[float] = None
    weight_unit: Optional[str] = None
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    dimension_unit: Optional[str] = None

    currency: Optional[str] = None
    base_price: Optional[float] = None
    sale_price: Optional[float] = None
    selling_price: Optional[float] = None
    special_price: Optional[float] = None

    stock_qty: Optional[int] = None
    stock_status: Optional[str] = None

    vendor_name: Optional[str] = None
    vendor_sku: Optional[str] = None

    short_description: Optional[str] = None
    long_description: Optional[str] = None

    meta_title: Optional[str] = None
    meta_description: Optional[str] = None
    search_keywords: Optional[str] = None

    certification: Optional[str] = None
    safety_standard: Optional[str] = None
    hazardous_material: Optional[str] = None
    prop65_warning: Optional[str] = None

    # FK
    industry_id: int = Field(foreign_key="industry.id")
    category_id: int = Field(foreign_key="category.id")

    # Relationships
    industry: Optional["Industry"] = Relationship(back_populates="products")
    category: Optional["Category"] = Relationship(back_populates="products")

    features: List["ProductFeature"] = Relationship(back_populates="product")
    images: List["ProductImage"] = Relationship(back_populates="product")
    videos: List["ProductVideo"] = Relationship(back_populates="product")
    documents: List["ProductDocument"] = Relationship(back_populates="product")
    attributes: List["ProductAttribute"] = Relationship(back_populates="product")


class ProductFeature(BaseModel, table=True):
    product_id: int = Field(foreign_key="product.id")
    name: str
    value: Optional[str] = None

    product: Product = Relationship(back_populates="features")


class ProductImage(BaseURLModel, table=True):
    product_id: int = Field(foreign_key="product.id")

    product: Product = Relationship(back_populates="images")


class ProductVideo(BaseURLModel, table=True):
    product_id: int = Field(foreign_key="product.id")

    product: Product = Relationship(back_populates="videos")


class ProductDocument(BaseURLModel, table=True):
    product_id: int = Field(foreign_key="product.id")

    product: Product = Relationship(back_populates="documents")


class ProductAttribute(BaseModel, table=True):
    product_id: int = Field(foreign_key="product.id")

    attribute_name: str
    attribute_value: Optional[str] = None
    attribute_uom: Optional[str] = None
    validation_value: Optional[str] = None
    validation_uom: Optional[str] = None

    product: Product = Relationship(back_populates="attributes")


class ProductSearchResult(BaseModel, table=True):
    """Stores search keywords and results for an API query"""

    url: Optional[str] = Field(default=None, nullable=True)
    total_result: Optional[int] = Field(default=None, nullable=True)
    query: Optional[Dict[str, Any]] = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )

    result: Optional[Dict[str, Any]] = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )
