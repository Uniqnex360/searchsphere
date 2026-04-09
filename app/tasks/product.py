import time
import os
import polars as pl
import pandas as pd

from datetime import datetime
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.database import get_sync_session
from app.models import (
    Industry,
    APPImport,
    Product,
    Category,
    ProductAttribute,
    ProductDocument,
    ProductFeature,
    ProductImage,
    ProductType,
    ProductVideo,
    Brand,
)
from app.services.category import SyncCategoryService
from app.celery_app import celery_app
from app.services import CeleryTaskStatus


def clean(value):
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


# =========================
# HANDLERS (SYNC)
# =========================


def handle_industry(df: pl.DataFrame, session: Session) -> dict:

    all_industries = df.get_column("industry_name").drop_nulls().unique().to_list()

    result = session.execute(
        select(Industry).where(Industry.industry_name.in_(all_industries))
    )
    existing = result.scalars().all()

    industry_map = {ind.industry_name: ind.id for ind in existing}

    missing = [name for name in all_industries if name not in industry_map]

    if missing:
        stmt = (
            insert(Industry)
            .values([{"industry_name": name} for name in missing])
            .on_conflict_do_nothing(index_elements=["industry_name"])
        )

        session.execute(stmt)
        session.commit()

        result = session.execute(
            select(Industry).where(Industry.industry_name.in_(missing))
        )
        new_rows = result.scalars().all()

        for ind in new_rows:
            industry_map[ind.industry_name] = ind.id

    return industry_map


def handle_brand(df: pl.DataFrame, session: Session) -> dict:

    all_brands = df.get_column("brand").drop_nulls().unique().to_list()

    result = session.execute(select(Brand).where(Brand.brand_name.in_(all_brands)))
    existing = result.scalars().all()

    brand_map = {obj.brand_name: obj.id for obj in existing}

    missing = [name for name in all_brands if name not in brand_map]

    if missing:
        stmt = (
            insert(Brand)
            .values([{"brand_name": name} for name in missing])
            .on_conflict_do_nothing(index_elements=["brand_name"])
        )

        session.execute(stmt)
        session.commit()

        result = session.execute(select(Brand).where(Brand.brand_name.in_(missing)))
        new_rows = result.scalars().all()

        for brand_obj in new_rows:
            brand_map[brand_obj.brand_name] = brand_obj.id

    return brand_map


def handle_product_type(df: pl.DataFrame, session: Session) -> dict:

    all_product_types = df.get_column("Product Type").drop_nulls().unique().to_list()

    result = session.execute(
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

        session.execute(stmt)
        session.commit()

        result = session.execute(
            select(ProductType).where(ProductType.product_type.in_(missing))
        )
        new_rows = result.scalars().all()

        for obj in new_rows:
            product_type_map[obj.product_type.strip().lower()] = obj.id

    return product_type_map


def handle_category(
    df: pl.DataFrame,
    session: Session,
    industry_map: dict,
    category_service,
) -> dict:

    unique_pairs = df.select(["industry_name", "taxonomy"]).unique().to_dicts()

    category_cache = {}

    for item in unique_pairs:
        industry_name = item.get("industry_name")
        taxonomy = item.get("taxonomy")

        if not taxonomy:
            continue

        cache_key = (industry_name or None, taxonomy)

        if cache_key in category_cache:
            continue

        industry_id = industry_map.get(industry_name) if industry_name else None

        parts = [p.strip() for p in taxonomy.split(">") if p.strip()]

        parent = None
        last_node = None

        for level_name in parts:
            node = category_service.get_or_create_node(
                db=session,
                name=level_name,
                industry_id=industry_id,
                parent=parent,
            )
            parent = node
            last_node = node

        category_cache[cache_key] = last_node

    return category_cache


# =========================
# CELERY TASK
# =========================


@celery_app.task(bind=True)
def import_products_task(self, file_path: str, obj_id: int):

    session: Session = get_sync_session()

    start_total = time.perf_counter()

    obj = session.get(APPImport, obj_id)

    try:
        batch_size = 200

        lf = pl.scan_csv(
            file_path,
            infer_schema_length=0,
            schema_overrides={"mpn": pl.Utf8, "upc": pl.Utf8},
        )

        df = lf.collect(streaming=True)
        total_rows = df.height
        obj.status = CeleryTaskStatus.STARTED
        obj.rows = total_rows
        session.commit()
        total_batches = (total_rows + batch_size - 1) // batch_size

        print(f"🚀 Total rows: {total_rows}, batches: {total_batches}")

        inserted, updated = 0, 0
        category_service = SyncCategoryService(Category)

        industry_map = handle_industry(df, session)

        category_map = handle_category(
            df=df,
            session=session,
            industry_map=industry_map,
            category_service=category_service,
        )

        brand_map = handle_brand(df=df, session=session)
        product_type_map = handle_product_type(df=df, session=session)

        last_progress = 0

        for batch_idx in range(total_batches):

            batch_start_time = time.perf_counter()

            start = batch_idx * batch_size
            end = min(start + batch_size, total_rows)
            batch_df = df.slice(start, end - start)

            print(f"\n📦 Batch {batch_idx+1}/{total_batches} rows {start}-{end}")

            rows = batch_df.to_dicts()

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

            mpns = set(r.get("mpn") for r in rows if r.get("mpn"))

            stmt = select(Product).where(Product.mpn.in_(mpns))
            result = session.execute(stmt)
            existing_products = result.scalars().all()

            product_map = {p.mpn: p for p in existing_products}

            products_to_delete_relations = []
            products_for_bg_task = []

            batch_inserted = 0
            batch_updated = 0

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

                product.weight = to_float(row_dict.get("weight"))
                product.length = to_float(row_dict.get("length"))
                product.width = to_float(row_dict.get("width"))
                product.height = to_float(row_dict.get("height"))
                product.base_price = to_float(row_dict.get("base_price"))
                product.sale_price = to_float(row_dict.get("sale_price"))
                product.selling_price = to_float(row_dict.get("selling_price"))
                product.special_price = to_float(row_dict.get("special_price"))
                product.stock_qty = to_int(row_dict.get("stock_qty"))

                industry_id = industry_map.get(row_dict.get("industry_name"))
                if industry_id:
                    product.industry_id = industry_id

                category_obj = category_map.get(
                    (row_dict.get("industry_name"), row_dict.get("taxonomy"))
                )
                if category_obj:
                    product.category_id = category_obj.id

                brand_id = brand_map.get(row_dict.get("brand"))
                if brand_id:
                    product.brand_id = brand_id

                pt = row_dict.get("Product Type")
                if pt:
                    pt = pt.strip().lower()
                pt_id = product_type_map.get(pt)
                if pt_id:
                    product.product_type_id = pt_id

                products_for_bg_task.append((product, row_dict))

            session.flush()

            if products_to_delete_relations:
                for tbl in [
                    "productimage",
                    "productvideo",
                    "productdocument",
                    "productfeature",
                    "productattribute",
                ]:
                    session.execute(
                        text(f"DELETE FROM {tbl} WHERE product_id = ANY(:pids)"),
                        {"pids": products_to_delete_relations},
                    )

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

            for i in range(0, len(light_relations), 200):
                session.add_all(light_relations[i : i + 200])
                session.flush()

            for i in range(0, len(attribute_relations), 100):
                session.add_all(attribute_relations[i : i + 100])
                session.flush()

            session.commit()

            progress = int(((batch_idx + 1) / total_batches) * 100)

            if progress - last_progress >= 20:
                last_progress = progress

                obj.meta_data = {
                    "current_batch": batch_idx + 1,
                    "total_batches": total_batches,
                    "progress": progress,
                }
                session.commit()

                self.update_state(state="PROGRESS", meta=obj.meta_data)

            print(f"✅ Batch {batch_idx+1}")

        total_time = round(time.perf_counter() - start_total, 2)

        obj.status = CeleryTaskStatus.SUCCESS
        obj.result = {
            "inserted": inserted,
            "updated": updated,
            "time": total_time,
        }
        obj.completed_at = datetime.utcnow()

        session.commit()

        return obj.result

    except Exception as e:
        obj.status = CeleryTaskStatus.FAILURE
        obj.error = str(e)
        obj.completed_at = datetime.utcnow()

        session.commit()
        raise e

    finally:
        session.close()
        if os.path.exists(file_path):
            os.remove(file_path)
