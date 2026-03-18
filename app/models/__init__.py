# models/__init__.py
from .base import BaseModel, BaseURLModel
from .common import MPTTBase

# Import models in an order that prevents circular imports
from .industry import Industry
from .category import Category
from .product import (
    Product,
    ProductImage,
    ProductVideo,
    ProductDocument,
    ProductFeature,
    ProductAttribute,
)

