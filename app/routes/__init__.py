from .category import router as category_router
from .upload import router as upload_router
from .product import router as product_router

routers = [category_router, upload_router, product_router]
