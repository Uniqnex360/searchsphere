import time
import os
import polars as pl
import pandas as pd

from datetime import datetime
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from app.es_client import get_es
from app.services import ESCollection
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


from datetime import datetime
from elasticsearch.helpers import streaming_bulk
from elasticsearch import Elasticsearch


def sync_product_suggest_data_es(
    es: Elasticsearch,
    products: list,
    autosuggest_index: str,
    product_index: str,
    batch_size: int = 30,
) -> dict:

    offset = 0
    total_processed = 0
    batch_number = 1

    print("🚀 Starting sync...")

    # ======================
    # Bulk helper with retry
    # ======================
    def log_bulk(actions, index_name):
        created = updated = failed = noop = 0
        if not actions:
            return created, updated, noop, failed

        failed_actions = []

        def generate():
            for action in actions:
                new_action = action.copy()
                new_action["_index"] = index_name
                yield new_action

        for ok, item in streaming_bulk(es, generate()):
            if not ok:
                print("❌ Bulk item failed:", item)
                failed += 1
                failed_actions.append(item)
                continue

            op_type = list(item.keys())[0]
            result = item[op_type].get("result")

            if result == "created":
                created += 1
            elif result == "updated":
                updated += 1
            elif result == "noop":
                noop += 1

        # Retry once
        if failed_actions:
            print(f"🔄 Retrying {len(failed_actions)} failed actions...")
            for ok, item in streaming_bulk(es, (i.copy() for i in failed_actions)):
                if not ok:
                    print("❌ Retry failed:", item)
                    failed += 1
                    continue

                op_type = list(item.keys())[0]
                result = item[op_type].get("result")

                if result == "created":
                    created += 1
                elif result == "updated":
                    updated += 1
                elif result == "noop":
                    noop += 1

        return created, updated, noop, failed

    # ======================
    # MAIN LOOP
    # ======================
    total_batches = (len(products) + batch_size - 1) // batch_size

    while offset < len(products):

        print(f"\n📦 Fetching batch {batch_number}/{total_batches}")

        batch_products = products[offset : offset + batch_size]

        autosuggest_actions = []
        product_actions = []

        for product in batch_products:

            brand = product.brand
            if not brand:
                continue

            category = product.category
            product_type_obj = product.product_type

            attributes = product.attributes or []
            features = product.features or []
            images = product.images or []
            videos = product.videos or []
            documents = product.documents or []

            now = datetime.utcnow()

            # --------------------
            # Product payload
            # --------------------
            data = {
                "product_name": product.product_name or "",
                "sku": product.sku or "",
                "mpn": product.mpn or "",
                "gtin": product.gtin or "",
                "ean": product.ean or "",
                "upc": product.upc or "",
                "product_type": (
                    product_type_obj.product_type if product_type_obj else ""
                ),
                "industry_name": (
                    product.industry.industry_name if product.industry else ""
                ),
                "category": category.name if category else "",
                "category_name": category.name if category else "",
                "taxonomy": product.taxonomy or "",
                "country_of_origin": product.country_of_origin or "",
                "warranty": product.warranty or "",
                "weight": product.weight or 0,
                "length": product.length or 0,
                "width": product.width or 0,
                "height": product.height or 0,
                "review": float(product.review) or 0,
                "base_price": product.base_price or 0,
                "sale_price": product.sale_price or 0,
                "selling_price": product.selling_price or 0,
                "special_price": product.special_price or 0,
                "weight_unit": product.weight_unit or "",
                "dimension_unit": product.dimension_unit or "",
                "currency": product.currency or "",
                "stock_status": product.stock_status or "",
                "vendor_name": product.vendor_name or "",
                "vendor_sku": product.vendor_sku or "",
                "short_description": product.short_description or "",
                "long_description": product.long_description or "",
                "meta_title": product.meta_title or "",
                "meta_description": product.meta_description or "",
                "search_keywords": product.search_keywords or "",
                "stock_qty": product.stock_qty or 0,
                "features": [
                    {"name": f.name or "", "value": f.value or ""} for f in features
                ],
                "attributes": [
                    {
                        "name": a.attribute_name or "",
                        "value": a.attribute_value or "",
                        "uom": a.attribute_uom or "",
                    }
                    for a in attributes
                ],
                "images": [{"name": i.name or "", "url": i.url or ""} for i in images],
                "videos": [{"name": v.name or "", "url": v.url or ""} for v in videos],
                "documents": [
                    {"name": d.name or "", "url": d.url or ""} for d in documents
                ],
            }

            # --------------------
            # Suggestions
            # --------------------
            brand_name = brand.brand_name or ""
            category_name = category.name if category else ""
            product_type_name = (
                product_type_obj.product_type if product_type_obj else ""
            )

            new_entry = f"{brand_name} {category_name}".strip()
            product_type_entry = (
                f"{brand_name} {category_name} {product_type_name}".strip()
            )

            suggest_set = {
                brand_name,
                new_entry,
                product_type_entry,
                product.product_name or "",
                product.mpn or "",
                product.sku or "",
            }

            for attr in attributes:
                full_entry = f"{brand_name} {category_name} {product_type_name} {attr.attribute_name or ''} {attr.attribute_value or ''}".strip()
                suggest_set.add(full_entry)

            # --------------------
            # AUTOSUGGEST
            # --------------------
            autosuggest_actions.append(
                {
                    "_op_type": "update",
                    "_id": brand.id,
                    "script": {
                        "source": """
                            if (ctx._source.brand_category == null) {
                                ctx._source.brand_category = params.new_entries;
                            } else if (!(ctx._source.brand_category instanceof List)) {
                                ctx._source.brand_category = [];
                            }
                            for (entry in params.new_entries) {
                                if (!ctx._source.brand_category.contains(entry)) {
                                    ctx._source.brand_category.add(entry);
                                }
                            }
                        """,
                        "params": {"new_entries": [new_entry]},
                    },
                    "upsert": {
                        "brand_name": brand_name,
                        "brand_category": [new_entry],
                    },
                }
            )

            # --------------------
            # PRODUCT INDEX (FIXED)
            # --------------------
            product_actions.append(
                {
                    "_op_type": "update",
                    "_id": product.id,
                    "script": {
                        "source": """
                            if (ctx._source.created_at == null) {
                                ctx._source.created_at = params.created_at;
                            }
                            ctx._source.updated_at = params.updated_at;

                            for (entry in params.doc.entrySet()) {
                                ctx._source[entry.getKey()] = entry.getValue();
                            }
                        """,
                        "params": {
                            "created_at": product.created_at,
                            "updated_at": now,
                            "doc": {
                                "brand": brand_name,
                                "suggest": list(suggest_set),
                                **data,
                            },
                        },
                    },
                    "upsert": {
                        "created_at": product.created_at,
                        "updated_at": now,
                        "brand": brand_name,
                        "suggest": list(suggest_set),
                        **data,
                    },
                }
            )

        # --------------------
        # BULK EXECUTION
        # --------------------
        print(
            f"Processing batch {batch_number} → "
            f"Products: {len(product_actions)}, Autosuggest: {len(autosuggest_actions)}"
        )

        p_created, p_updated, p_noop, p_failed = log_bulk(
            product_actions, product_index
        )
        a_created, a_updated, a_noop, a_failed = log_bulk(
            autosuggest_actions, autosuggest_index
        )

        print(
            f"Batch {batch_number} → PRODUCTS: C:{p_created} U:{p_updated} F:{p_failed}"
        )
        print(
            f"Batch {batch_number} → AUTOSUGGEST: C:{a_created} U:{a_updated} F:{a_failed}"
        )

        total_processed += len(batch_products)
        offset += batch_size
        batch_number += 1

    print(f"\n🎉 Sync completed! Total processed: {total_processed}")

    return {"total_processed": total_processed}


# =========================
# CELERY TASK
# =========================


@celery_app.task(bind=True)
def import_products_task(self, file_path: str, obj_id: int):

    session: Session = get_sync_session()

    start_total = time.perf_counter()

    obj = session.get(APPImport, obj_id)

    es = get_es()

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
                    "review",
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

            sync_product_suggest_data_es(
                es,
                [p for p, _ in products_for_bg_task],
                ESCollection.PRODUCT_AUTO_SUGGEST_V7.value,
                ESCollection.PRODUCT_V7.value,
            )

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
