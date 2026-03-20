import pandas as pd
from elasticsearch import Elasticsearch
from qdrant_client import QdrantClient
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.future import select
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
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    es_service = ElasticsearchService(es, "product_vector")

    df = pd.read_csv(
        file.file,
        sep=",",
        engine="python",
        encoding="latin-1",
        dtype=str,
        skipinitialspace=True,
    )

    # Convert ALL NaN → None
    df = df.where(pd.notna(df), None)

    inserted, updated = 0, 0
    category_service = CategoryService(Category)

    for _, row in df.iterrows():

        row = {k: clean(v) for k, v in row.items()}

        mpn = row.get("mpn")
        product_name = row.get("product_name")
        stmt = select(Product).where(
            Product.mpn == mpn, Product.product_name == product_name
        )

        result = await session.execute(stmt)
        existing_product = result.scalar_one_or_none()

        if existing_product:
            product = existing_product
            updated += 1
        else:
            product = Product(mpn=mpn, product_name=product_name)
            inserted += 1

        # ---- Update product fields ----

        product.sku = row.get("sku")
        product.brand = row.get("brand")
        product.gtin = row.get("gtin")
        product.ean = row.get("ean")
        product.upc = row.get("upc")
        product.taxonomy = row.get("taxonomy")
        product.country_of_origin = row.get("country_of_origin")
        product.warranty = row.get("warranty")

        product.weight = to_float(row.get("weight"))
        product.weight_unit = row.get("weight_unit")

        product.length = to_float(row.get("length"))
        product.width = to_float(row.get("width"))
        product.height = to_float(row.get("height"))

        product.dimension_unit = row.get("dimension_unit")
        product.currency = row.get("currency")

        product.base_price = to_float(row.get("base_price"))
        product.sale_price = to_float(row.get("sale_price"))
        product.selling_price = to_float(row.get("selling_price"))
        product.special_price = to_float(row.get("special_price"))

        product.stock_qty = to_int(row.get("stock_qty"))
        product.stock_status = row.get("stock_status")

        product.vendor_name = row.get("vendor_name")
        product.vendor_sku = row.get("vendor_sku")

        product.short_description = row.get("short_description")
        product.long_description = row.get("long_description")

        product.meta_title = row.get("meta_title")
        product.meta_description = row.get("meta_description")
        product.search_keywords = row.get("search_keywords")

        product.certification = row.get("certification")
        product.safety_standard = row.get("safety_standard")
        product.hazardous_material = row.get("hazardous_material")
        product.prop65_warning = row.get("prop65_warning")

        # ---- Industry ----

        industry_obj, _ = await get_or_create(
            db=session,
            model=Industry,
            industry_name=row.get("industry_name"),
        )

        product.industry_id = industry_obj.id

        # ---- Category ----

        category_obj = await category_service.create_from_path(
            session,
            industry_name=industry_obj.industry_name,
            path=row.get("taxonomy"),
        )

        product.category_id = category_obj.id

        if not existing_product:
            session.add(product)

        else:
            # delete previous related rows
            await session.exec(
                ProductImage.__table__.delete().where(
                    ProductImage.product_id == product.id
                )
            )
            await session.exec(
                ProductVideo.__table__.delete().where(
                    ProductVideo.product_id == product.id
                )
            )
            await session.exec(
                ProductDocument.__table__.delete().where(
                    ProductDocument.product_id == product.id
                )
            )
            await session.exec(
                ProductFeature.__table__.delete().where(
                    ProductFeature.product_id == product.id
                )
            )
            await session.exec(
                ProductAttribute.__table__.delete().where(
                    ProductAttribute.product_id == product.id
                )
            )

            await session.flush()

            await session.refresh(
                product,
                attribute_names=[
                    "category",
                    "industry",
                    "features",
                    "images",
                    "videos",
                    "documents",
                    "attributes",
                ],
            )

        # ---- Images ----

        for i in range(1, 9):
            name = row.get(f"image_name_{i}")
            url = row.get(f"image_url_{i}")

            if name and url:
                session.add(ProductImage(product=product, name=name, url=url))

        # ---- Videos ----

        for i in range(1, 4):
            name = row.get(f"video_name_{i}")
            url = row.get(f"video_url_{i}")

            if name and url:
                session.add(ProductVideo(product=product, name=name, url=url))

        # ---- Documents ----

        for i in range(1, 6):
            name = row.get(f"document_name_{i}")
            url = row.get(f"document_url_{i}")

            if name and url:
                session.add(ProductDocument(product=product, name=name, url=url))

        # ---- Features ----

        for i in range(1, 11):
            value = row.get(f"features_{i}")

            if value:
                session.add(
                    ProductFeature(product=product, name=f"features_{i}", value=value)
                )

        # ---- Attributes ----

        for i in range(1, 41):
            name = row.get(f"attribute_name{i}")

            if name and name.strip():
                session.add(
                    ProductAttribute(
                        product=product,
                        attribute_name=name,
                        attribute_value=row.get(f"attribute_value{i}"),
                        attribute_uom=row.get(f"attribute_uom{i}"),
                        validation_value=row.get(f"validation_value{i}"),
                        validation_uom=row.get(f"validation_uom{i}"),
                    )
                )

        # updates the data in elasticsearch
        background_tasks.add_task(
            sync_product_with_es_qdrant, es_service, product.id, product, qdrant
        )

    await session.commit()

    return {
        "status": "success",
        "products_inserted": inserted,
        "products_updated": updated,
    }
