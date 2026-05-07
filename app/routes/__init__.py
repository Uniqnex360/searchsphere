from .category import router as category_router
from .upload import router as upload_router
from .product import router as product_router
from .app_import import router as app_import
from .dashboard import router as dashboard
from .es_mapping import router as es_mapping
from .shopify_oauth import router as shopify_oauth

routers = [
    category_router,
    upload_router,
    product_router,
    app_import,
    dashboard,
    es_mapping,
    shopify_oauth,
]
