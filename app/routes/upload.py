import time
import os
import time
import tempfile
import polars as pl
import pandas as pd

from sqlalchemy.dialects.postgresql import insert
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
    Brand,
    ProductType,
)
from app.services import (
    CategoryService,
    ElasticsearchService,
    get_or_create,
    sync_with_product,
    sync_with_vector_product,
    sync_product_with_es_qdrant,
    get_qdrant_client,
    ESCollection,
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

    es_service = ElasticsearchService(es, ESCollection.PRODUCT_V2.value)

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


@router.post("/v2/upload-products-csv/")
async def upload_products_csv_v2(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    es: Elasticsearch = Depends(get_es),
    qdrant: QdrantClient = Depends(get_qdrant_client),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    es_service = ElasticsearchService(es, ESCollection.PRODUCT_V2.value)

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

    industry_cache = {}
    category_cache = {}

    for row in df.itertuples(index=False):
        row_dict = {k: clean(getattr(row, k)) for k in row._fields}

        mpn = row_dict.get("mpn")
        product_name = row_dict.get("product_name")

        print("processing product", product_name)

        # ---- FETCH EXISTING PRODUCT ----
        stmt = select(Product).where(
            Product.mpn == mpn,
            Product.product_name == product_name,
        )
        result = await session.execute(stmt)
        existing_product = result.scalars().first()

        if existing_product:
            product = existing_product
            updated += 1

            # delete relations for this product
            for tbl in [
                "productimage",
                "productvideo",
                "productdocument",
                "productfeature",
                "productattribute",
            ]:
                await session.execute(
                    text(f"DELETE FROM {tbl} WHERE product_id = :pid"),
                    {"pid": product.id},
                )
        else:
            product = Product(mpn=mpn, product_name=product_name)
            inserted += 1

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

        # ---- NUMERIC ----
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
        print("industry name ===", industry_name)
        if industry_name in industry_cache:
            industry_obj = industry_cache[industry_name]
        else:
            industry_obj, _ = await get_or_create(
                db=session, model=Industry, industry_name=industry_name.strip()
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
                session,
                industry_name=industry_obj.industry_name,
                path=taxonomy,
            )
            category_cache[category_key] = category_obj

        product.category_id = category_obj.id
        session.add(product)

        # ---- FLUSH TO GET ID ----
        await session.flush()

        # ---- RELATIONS ----
        relation_objects = []

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
                        product_id=product.id,
                        name=f"features_{i}",
                        value=value,
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

        session.add_all(relation_objects)

        # ---- COMMIT PER ROW ----
        await session.commit()

    return {
        "status": "success",
        "products_inserted": inserted,
        "products_updated": updated,
    }


async def handle_industry(
    df: pl.DataFrame,
    session: AsyncSession,
) -> dict:
    """get all the unique industry from the csv file and create or update them and return the indsutry map"""

    # ============================================================
    # 🔥 STEP 1: Extract ALL unique industries (once)
    # ============================================================
    all_industries = df.get_column("industry_name").drop_nulls().unique().to_list()

    # ============================================================
    # 🔥 STEP 2: Fetch existing industries
    # ============================================================
    result = await session.exec(
        select(Industry).where(Industry.industry_name.in_(all_industries))
    )
    existing = result.scalars().all()

    industry_map = {ind.industry_name: ind.id for ind in existing}

    # ============================================================
    # 🔥 STEP 3: Insert missing industries (UPSERT)
    # ============================================================
    missing = [name for name in all_industries if name not in industry_map]

    if missing:
        stmt = (
            insert(Industry)
            .values([{"industry_name": name} for name in missing])
            .on_conflict_do_nothing(index_elements=["industry_name"])
        )

        await session.exec(stmt)
        await session.commit()

        # fetch newly created
        result = await session.exec(
            select(Industry).where(Industry.industry_name.in_(missing))
        )
        new_rows = result.scalars().all()

        for ind in new_rows:
            industry_map[ind.industry_name] = ind.id

    return industry_map


async def handle_brand(
    df: pl.DataFrame,
    session: AsyncSession,
) -> dict:
    """get all the unique brand from the csv file create or them and return brand map"""

    all_brands = df.get_column("brand").drop_nulls().unique().to_list()

    result = await session.exec(select(Brand).where(Brand.brand_name.in_(all_brands)))
    existing = result.scalars().all()

    brand_map = {obj.brand_name: obj.id for obj in existing}

    missing = [name for name in all_brands if name not in brand_map]

    if missing:
        stmt = (
            insert(Brand)
            .values([{"brand_name": name} for name in missing])
            .on_conflict_do_nothing(index_elements=["brand_name"])
        )

        await session.exec(stmt)
        await session.commit()

        # fetch newly created
        result = await session.exec(select(Brand).where(Brand.brand_name.in_(missing)))
        new_rows = result.scalars().all()

        for brand_obj in new_rows:
            brand_map[brand_obj.brand_name] = brand_obj.id

    return brand_map


async def handle_product_type(
    df: pl.DataFrame,
    session: AsyncSession,
) -> dict:
    """get all the product types from the csv file and creat or update return product type map"""

    all_product_types = df.get_column("Product Type").drop_nulls().unique().to_list()
    result = await session.exec(
        select(ProductType).where(ProductType.product_type.in_(all_product_types))
    )
    existing = result.scalars().all()
    product_type_map = {obj.product_type.strip().lower(): obj.id for obj in existing}

    missing = [
        name
        for name in all_product_types
        if name and name.strip().lower() not in product_type_map
    ]

    if missing:
        stmt = (
            insert(ProductType)
            .values([{"product_type": name} for name in missing])
            .on_conflict_do_nothing(index_elements=["product_type"])
        )

        await session.exec(stmt)
        await session.commit()

        # fetch newly created
        result = await session.exec(
            select(ProductType).where(ProductType.product_type.in_(missing))
        )
        new_rows = result.scalars().all()

        for obj in new_rows:
            product_type_map[obj.product_type.strip().lower()] = obj.id

    return product_type_map


async def handle_category(
    df: pl.DataFrame,
    session: AsyncSession,
    industry_map: dict,
    category_service,
) -> dict:
    """
    Build all categories from CSV in bulk and return mapping:
    (industry_name, taxonomy) -> category_obj
    """

    # ============================================================
    # 🔥 STEP 1: Extract unique (industry, taxonomy)
    # ============================================================
    unique_pairs = df.select(["industry_name", "taxonomy"]).unique().to_dicts()

    category_cache = {}

    # ============================================================
    # 🔥 STEP 2: Process each unique path ONCE
    # ============================================================
    for item in unique_pairs:
        industry_name = item.get("industry_name")
        taxonomy = item.get("taxonomy")

        if not taxonomy:
            continue

        # ✅ allow industry_name to be None
        cache_key = (industry_name or None, taxonomy)

        if cache_key in category_cache:
            continue

        # ✅ Resolve industry_id (can be None now)
        industry_id = industry_map.get(industry_name) if industry_name else None

        # ========================================================
        # 🔥 Build category path
        # ========================================================
        parts = [p.strip() for p in taxonomy.split(">") if p.strip()]

        parent = None
        last_node = None

        for level_name in parts:
            node = await category_service.get_or_create_node(
                db=session,
                name=level_name,
                industry_id=industry_id,  # ✅ can be None
                parent=parent,
            )
            parent = node
            last_node = node

        category_cache[cache_key] = last_node

    return category_cache


@router.post("/v3/upload-products-csv/")
async def upload_products_csv_v3(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    es: Elasticsearch = Depends(get_es),
    qdrant: QdrantClient = Depends(get_qdrant_client),
):
    start_total = time.perf_counter()

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    batch_size = 200
    es_service = ElasticsearchService(es, ESCollection.PRODUCT_V2.value)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(await file.read())
        temp_path = tmp.name

    try:
        lf = pl.scan_csv(
            temp_path,
            infer_schema_length=0,
            schema_overrides={"mpn": pl.Utf8, "upc": pl.Utf8},
        )

        df = lf.collect(streaming=True)
        total_rows = df.height
        total_batches = (total_rows + batch_size - 1) // batch_size

        print(f"🚀 Total rows: {total_rows}, batches: {total_batches}")

        inserted, updated = 0, 0
        category_service = CategoryService(Category)

        # 🔥 STEP 1: HANDLE INDUSTRY (BULK)
        industry_start = time.perf_counter()
        industry_map = await handle_industry(df, session)
        print(f"⏱ industry time: {round(time.perf_counter() - industry_start,2)}s")

        # 🔥 STEP 2: HANDLE CATEGORY (BULK)
        category_start = time.perf_counter()
        category_map = await handle_category(
            df=df,
            session=session,
            industry_map=industry_map,
            category_service=category_service,
        )
        print(f"⏱ category time: {round(time.perf_counter() - category_start,2)}s")

        # 🔥 STEP 3: HANDLE BRAND (BULK)
        brand_start = time.perf_counter()
        brand_map = await handle_brand(df=df, session=session)
        print(f"⏱ brand time: {round(time.perf_counter() - brand_start,2)}s")

        # 🔥 STEP 4: HANDLE Product Type (BULK)
        product_type_start = time.perf_counter()
        product_type_map = await handle_product_type(df=df, session=session)
        print(
            f"⏱ product type time: {round(time.perf_counter() - product_type_start,2)}s"
        )

        # ============================
        # 🔁 BATCH LOOP
        # ============================
        for batch_idx in range(total_batches):
            batch_start_time = time.perf_counter()

            start = batch_idx * batch_size
            end = min(start + batch_size, total_rows)
            batch_df = df.slice(start, end - start)

            print(f"\n📦 Batch {batch_idx+1}/{total_batches} rows {start}-{end}")

            rows = batch_df.to_dicts()

            # 🔥 DEDUPE INSIDE BATCH (CRITICAL)
            seen_mpns = set()
            filtered_rows = []

            for r in rows:
                mpn = r.get("mpn")
                if mpn:
                    if mpn in seen_mpns:
                        continue
                    seen_mpns.add(mpn)
                filtered_rows.append(r)

            rows = filtered_rows

            # ---- FETCH EXISTING PRODUCTS (mpn-based) ----
            mpns = set(r.get("mpn") for r in rows if r.get("mpn"))

            stmt = select(Product).where(Product.mpn.in_(mpns))
            result = await session.execute(stmt)
            existing_products = result.scalars().all()

            product_map = {p.mpn: p for p in existing_products}

            products_to_delete_relations = []
            products_for_bg_task = []

            batch_inserted = 0
            batch_updated = 0

            # ============================
            # 🔁 PROCESS ROWS
            # ============================
            for row_dict in rows:
                row_dict = {k: clean(v) for k, v in row_dict.items()}

                mpn = row_dict.get("mpn")
                product_name = row_dict.get("product_name")

                existing_product = product_map.get(mpn)

                if existing_product:
                    product = existing_product
                    batch_updated += 1
                    updated += 1
                    products_to_delete_relations.append(product.id)
                else:
                    product = Product(mpn=mpn, product_name=product_name)
                    session.add(product)
                    batch_inserted += 1
                    inserted += 1

                # ---- FIELDS ----
                for field in [
                    "sku",
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

                # ---- NUMERIC ----
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
                industry_id = industry_map.get(industry_name)
                if industry_id:
                    product.industry_id = industry_id

                # ---- CATEGORY ----
                taxonomy = row_dict.get("taxonomy")
                category_key = (industry_name, taxonomy)
                category_obj = category_map.get(category_key)
                if category_obj:
                    product.category_id = category_obj.id

                # --- Brand ----
                brand_name = row_dict.get("brand")
                brand_id = brand_map.get(brand_name)
                if brand_id:
                    product.brand_id = brand_id

                # --- Product Type ----
                product_type = row_dict.get("Product Type")
                if product_type:
                    product_type = product_type.strip().lower()
                product_type_id = product_type_map.get(product_type)
                if product_type_id:
                    product.product_type_id = product_type_id

                products_for_bg_task.append((product, row_dict))

            # ---- FLUSH FIRST (IMPORTANT) ----
            await session.flush()

            # ---- DELETE OLD RELATIONS (AFTER FLUSH) ----
            if products_to_delete_relations:
                for tbl in [
                    "productimage",
                    "productvideo",
                    "productdocument",
                    "productfeature",
                    "productattribute",
                ]:
                    await session.execute(
                        text(f"DELETE FROM {tbl} WHERE product_id = ANY(:pids)"),
                        {"pids": products_to_delete_relations},
                    )

            # ============================
            # 🔁 ADD RELATIONS
            # ============================
            light_relations = []
            attribute_relations = []

            for product, row_dict in products_for_bg_task:

                for i in range(1, 9):
                    if row_dict.get(f"image_name_{i}") and row_dict.get(
                        f"image_url_{i}"
                    ):
                        light_relations.append(
                            ProductImage(
                                product_id=product.id,
                                name=row_dict[f"image_name_{i}"],
                                url=row_dict[f"image_url_{i}"],
                            )
                        )

                for i in range(1, 4):
                    if row_dict.get(f"video_name_{i}") and row_dict.get(
                        f"video_url_{i}"
                    ):
                        light_relations.append(
                            ProductVideo(
                                product_id=product.id,
                                name=row_dict[f"video_name_{i}"],
                                url=row_dict[f"video_url_{i}"],
                            )
                        )

                for i in range(1, 6):
                    if row_dict.get(f"document_name_{i}") and row_dict.get(
                        f"document_url_{i}"
                    ):
                        light_relations.append(
                            ProductDocument(
                                product_id=product.id,
                                name=row_dict[f"document_name_{i}"],
                                url=row_dict[f"document_url_{i}"],
                            )
                        )

                for i in range(1, 11):
                    if row_dict.get(f"features_{i}"):
                        light_relations.append(
                            ProductFeature(
                                product_id=product.id,
                                name=f"features_{i}",
                                value=row_dict[f"features_{i}"],
                            )
                        )

                for i in range(1, 41):
                    name = row_dict.get(f"attribute_name{i}")
                    if name and name.strip():
                        attribute_relations.append(
                            ProductAttribute(
                                product_id=product.id,
                                attribute_name=name,
                                attribute_value=row_dict.get(f"attribute_value{i}"),
                                attribute_uom=row_dict.get(f"attribute_uom{i}"),
                                validation_value=row_dict.get(f"validation_value{i}"),
                                validation_uom=row_dict.get(f"validation_uom{i}"),
                            )
                        )

            # ----------------------------
            # LIGHT RELATIONS (safe)
            # ----------------------------
            RELATION_CHUNK_SIZE = 200

            for i in range(0, len(light_relations), RELATION_CHUNK_SIZE):
                chunk = light_relations[i : i + RELATION_CHUNK_SIZE]
                session.add_all(chunk)
                await session.flush()

            # ----------------------------
            # ATTRIBUTES (heavy 🔥)
            # ----------------------------
            ATTRIBUTE_CHUNK_SIZE = 100  # 🔥 smaller = safer

            for i in range(0, len(attribute_relations), ATTRIBUTE_CHUNK_SIZE):
                chunk = attribute_relations[i : i + ATTRIBUTE_CHUNK_SIZE]
                session.add_all(chunk)
                await session.flush()

            # ---- COMMIT ----
            await session.commit()

            batch_time = round(time.perf_counter() - batch_start_time, 2)

            print(
                f"✅ Batch {batch_idx+1} | "
                f"Inserted: {batch_inserted} | "
                f"Updated: {batch_updated} | "
                f"Time: {batch_time}s"
            )

        total_time = round(time.perf_counter() - start_total, 2)

        print(f"\n🔥 TOTAL TIME: {total_time}s")

        return {
            "status": "success",
            "products_inserted": inserted,
            "products_updated": updated,
            "total_batches": total_batches,
            "total_time": total_time,
        }

    finally:
        os.remove(temp_path)
