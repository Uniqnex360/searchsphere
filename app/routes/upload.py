import time
import pandas as pd
from elasticsearch import Elasticsearch
from qdrant_client import QdrantClient
from sqlalchemy import text, tuple_
from sqlalchemy.future import select
from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, BackgroundTasks

from app.database import get_session
from app.models import (
    Product,
    ProductImage,
    ProductVideo,
    ProductDocument,
    ProductFeature,
    ProductAttribute,
    Industry,
    Category,
)
from app.services import (
    CategoryService,
    ElasticsearchService,
    get_or_create,
    sync_with_product,
    sync_with_vector_product,
    sync_product_with_es_qdrant,
    get_qdrant_client,
)
from app.es_client import get_es

router = APIRouter()


def clean(value):
    """Convert pandas NaN/empty to None"""
    if pd.isna(value):
        return None
    return value


def to_float(value):
    if pd.isna(value) or value is None or value == "":
        return None
    return float(value)


def to_int(value):
    if pd.isna(value) or value is None or value == "":
        return None
    return int(value)


@router.post("/upload-products-csv/")
async def upload_products_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    es: Elasticsearch = Depends(get_es),
    qdrant: QdrantClient = Depends(get_qdrant_client),
):
    start_total = time.perf_counter()

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    es_service = ElasticsearchService(es, "product_vector")

    # ---- CSV READ ----
    df = pd.read_csv(
        file.file,
        sep=",",
        engine="python",
        encoding="latin-1",
        dtype=str,
        skipinitialspace=True,
    )
    df = df.where(pd.notna(df), None)

    inserted, updated = 0, 0
    category_service = CategoryService(Category)

    # ---- PREPARE PRODUCT MAP ----
    keys = set((row.mpn, row.product_name) for row in df.itertuples(index=False))
    stmt = select(Product).where(tuple_(Product.mpn, Product.product_name).in_(keys))
    result = await session.execute(stmt)
    existing_products = result.scalars().all()
    product_map = {(p.mpn, p.product_name): p for p in existing_products}

    # ---- CACHES ----
    industry_cache = {}
    category_cache = {}

    # Collect IDs for deleting old relations
    products_to_delete_relations = []

    # ---- PROCESS EACH ROW ----
    products_for_bg_task = []
    for row in df.itertuples(index=False):
        row_dict = {k: clean(getattr(row, k)) for k in row._fields}

        mpn = row_dict.get("mpn")
        product_name = row_dict.get("product_name")
        existing_product = product_map.get((mpn, product_name))
        print("processing product", product_name)
        if existing_product:
            product = existing_product
            updated += 1
            products_to_delete_relations.append(product.id)
        else:
            product = Product(mpn=mpn, product_name=product_name)
            inserted += 1
            session.add(product)

        # ---- UPDATE FIELDS ----
        for field in [
            "sku",
            "brand",
            "gtin",
            "ean",
            "upc",
            "taxonomy",
            "country_of_origin",
            "warranty",
            "weight_unit",
            "dimension_unit",
            "currency",
            "stock_status",
            "vendor_name",
            "vendor_sku",
            "short_description",
            "long_description",
            "meta_title",
            "meta_description",
            "search_keywords",
            "certification",
            "safety_standard",
            "hazardous_material",
            "prop65_warning",
        ]:
            setattr(product, field, row_dict.get(field))

        # Numeric fields
        product.weight = to_float(row_dict.get("weight"))
        product.length = to_float(row_dict.get("length"))
        product.width = to_float(row_dict.get("width"))
        product.height = to_float(row_dict.get("height"))
        product.base_price = to_float(row_dict.get("base_price"))
        product.sale_price = to_float(row_dict.get("sale_price"))
        product.selling_price = to_float(row_dict.get("selling_price"))
        product.special_price = to_float(row_dict.get("special_price"))
        product.stock_qty = to_int(row_dict.get("stock_qty"))

        # ---- INDUSTRY ----
        industry_name = row_dict.get("industry_name")
        if industry_name in industry_cache:
            industry_obj = industry_cache[industry_name]
        else:
            industry_obj, _ = await get_or_create(
                db=session, model=Industry, industry_name=industry_name
            )
            industry_cache[industry_name] = industry_obj
        product.industry_id = industry_obj.id

        # ---- CATEGORY ----
        taxonomy = row_dict.get("taxonomy")
        category_key = (industry_obj.industry_name, taxonomy)
        if category_key in category_cache:
            category_obj = category_cache[category_key]
        else:
            category_obj = await category_service.create_from_path(
                session, industry_name=industry_obj.industry_name, path=taxonomy
            )
            category_cache[category_key] = category_obj
        product.category_id = category_obj.id

        products_for_bg_task.append((product, row_dict))

    # ---- DELETE OLD RELATIONS BEFORE COMMIT ----
    if products_to_delete_relations:
        tables = [
            "productimage",
            "productvideo",
            "productdocument",
            "productfeature",
            "productattribute",
        ]
        for tbl in tables:
            await session.execute(
                text(f"DELETE FROM {tbl} WHERE product_id = ANY(:pids)"),
                {"pids": products_to_delete_relations},
            )

    # ---- FLUSH TO GET PRODUCT IDs ----
    await session.flush()

    # ---- ADD RELATIONS ----
    relation_objects = []
    for product, row_dict in products_for_bg_task:
        # IMAGES
        for i in range(1, 9):
            name, url = row_dict.get(f"image_name_{i}"), row_dict.get(f"image_url_{i}")
            if name and url:
                relation_objects.append(
                    ProductImage(product_id=product.id, name=name, url=url)
                )
        # VIDEOS
        for i in range(1, 4):
            name, url = row_dict.get(f"video_name_{i}"), row_dict.get(f"video_url_{i}")
            if name and url:
                relation_objects.append(
                    ProductVideo(product_id=product.id, name=name, url=url)
                )
        # DOCUMENTS
        for i in range(1, 6):
            name, url = row_dict.get(f"document_name_{i}"), row_dict.get(
                f"document_url_{i}"
            )
            if name and url:
                relation_objects.append(
                    ProductDocument(product_id=product.id, name=name, url=url)
                )
        # FEATURES
        for i in range(1, 11):
            value = row_dict.get(f"features_{i}")
            if value:
                relation_objects.append(
                    ProductFeature(
                        product_id=product.id, name=f"features_{i}", value=value
                    )
                )
        # ATTRIBUTES
        for i in range(1, 41):
            name = row_dict.get(f"attribute_name{i}")
            if name and name.strip():
                relation_objects.append(
                    ProductAttribute(
                        product_id=product.id,
                        attribute_name=name,
                        attribute_value=row_dict.get(f"attribute_value{i}"),
                        attribute_uom=row_dict.get(f"attribute_uom{i}"),
                        validation_value=row_dict.get(f"validation_value{i}"),
                        validation_uom=row_dict.get(f"validation_uom{i}"),
                    )
                )

    # ---- BULK ADD RELATIONS ----
    session.add_all(relation_objects)

    # ---- COMMIT ALL ----
    await session.commit()

    # ---- SCHEDULE BACKGROUND TASKS AFTER COMMIT ----
    for product, _ in products_for_bg_task:
        background_tasks.add_task(
            sync_product_with_es_qdrant,
            es_service,
            product.id,
            product,
            qdrant,
            session,
        )

    return {
        "status": "success",
        "products_inserted": inserted,
        "products_updated": updated,
    }
