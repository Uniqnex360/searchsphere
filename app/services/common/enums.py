from enum import Enum


class QDrantCollection(str, Enum):
    """collection names used for qdrant """
    
    PRODUCT = "product"


class ESCollection(str, Enum):
    """collection names used in elastic search collections"""

    PRODUCT = "product_vector"