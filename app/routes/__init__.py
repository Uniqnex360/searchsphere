from .category import router as category_router
from .upload import router as upload_router
from .product import router as product_router
from .app_import import router as app_import
from .dashboard import router as dashboard

routers = [category_router, upload_router, product_router, app_import, dashboard]
