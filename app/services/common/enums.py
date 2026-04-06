from enum import Enum


class QDrantCollection(str, Enum):
    """collection names used for qdrant"""

    PRODUCT = "product"


class ESCollection(str, Enum):
    """collection names used in elastic search collections"""

    PRODUCT = "product_vector"
    PRODUCT_V2 = "product_v2"
    PRODUCT_V5 = "product_v5"
    PRODUCT_AUTO_SUGGEST_V6 = "product_auto_suggest_v6"
    PRODUCT_V6 = "product_v6"
