from itertools import permutations
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlmodel import select

from app.services import ElasticsearchService
from app.models import Product


def build_suggestions_chain(brand=None, product_type=None, category=None):
    """
    Build autocomplete suggestions using permutations of brand, product_type, category.
    Returns a list of unique suggestions.
    """
    parts = [p for p in [brand, product_type, category] if p]

    # Generate all non-empty permutations
    suggestions = set()
    for r in range(1, len(parts) + 1):
        for perm in permutations(parts, r):
            suggestions.add(" ".join(perm))

    return list(suggestions)


# async def sync_products_to_es_v5(
#     session: AsyncSession,
#     es_service: ElasticsearchService,
#     batch_size: int = 300,
# ):
#     offset = 0
#     total_indexed = 0

#     while True:
#         result = await session.execute(
#             select(Product)
#             .options(
#                 selectinload(Product.images),
#                 selectinload(Product.features),
#                 selectinload(Product.attributes),
#                 selectinload(Product.videos),
#                 selectinload(Product.documents),
#                 selectinload(Product.category),
#                 selectinload(Product.industry),
#                 selectinload(Product.brand),
#                 selectinload(Product.product_type),
#             )
#             .offset(offset)
#             .limit(batch_size)
#         )

#         products = result.scalars().all()
#         if not products:
#             break

#         print(f"📦 Processing batch offset={offset}, size={len(products)}")

#         actions = []

#         for product in products:
#             brand = product.brand.brand_name if product.brand else None
#             product_type = (
#                 product.product_type.product_type if product.product_type else None
#             )
#             category = product.category.name if product.category else None

#             attribute_values = [
#                 attr.attribute_value
#                 for attr in product.attributes
#                 if attr.attribute_value
#             ]

#             # 🔥 Build global suggestion parts using permutations
#             all_suggestions = build_suggestions_chain(
#                 brand=brand,
#                 product_type=product_type,
#                 category=category,
#             )

#             # Append MPN, SKU, and attribute values at the end
#             all_suggestions += [v for v in [product.mpn, product.sku] if v]

#             # Deduplicate suggestions
#             all_suggestions = list(dict.fromkeys(all_suggestions))

#             data = {
#                 # 🔍 main searchable fields
#                 "product_name": product.product_name,
#                 # identifiers
#                 "sku": product.sku,
#                 "mpn": product.mpn,
#                 "gtin": product.gtin,
#                 "ean": product.ean,
#                 "upc": product.upc,
#                 # relations
#                 "brand": brand,
#                 "product_type": product_type,
#                 "category": category,
#                 "category_name": category,
#                 "industry_name": (
#                     product.industry.industry_name if product.industry else None
#                 ),
#                 # 🔥 SUGGESTION FIELDS
#                 "brand_suggest": brand,
#                 "product_type_suggest": product_type,
#                 "category_suggest": category,
#                 "mpn_suggest": product.mpn,
#                 "attribute_value_suggest": attribute_values,
#                 # 🔥 GLOBAL AUTOCOMPLETE
#                 "all_suggestions": all_suggestions,
#                 # misc
#                 "taxonomy": product.taxonomy,
#                 "vendor_name": product.vendor_name,
#                 # pricing
#                 "base_price": product.base_price,
#                 "sale_price": product.sale_price,
#                 # descriptions
#                 "short_description": product.short_description,
#                 "long_description": product.long_description,
#                 # stock
#                 "stock_qty": product.stock_qty,
#                 # nested
#                 "features": [
#                     {"name": f.name, "value": f.value} for f in product.features
#                 ],
#                 "attributes": [
#                     {
#                         "name": attr.attribute_name,
#                         "value": attr.attribute_value,
#                         "uom": attr.attribute_uom,
#                     }
#                     for attr in product.attributes
#                 ],
#                 "images": [
#                     {"name": img.name, "url": img.url} for img in product.images
#                 ],
#             }

#             actions.append(
#                 {
#                     "_op_type": "index",
#                     "_id": product.id,
#                     "_source": data,
#                 }
#             )

#         es_service.bulk(actions)

#         total_indexed += len(products)
#         offset += batch_size
#         print(f"✅ Indexed so far: {total_indexed}")


#     print(f"\n🔥 REINDEX COMPLETE: {total_indexed} products")


async def sync_products_to_es_v5(
    session: AsyncSession,
    es_service: ElasticsearchService,
    batch_size: int = 300,
):
    offset = 0
    total_indexed = 0

    while True:
        result = await session.execute(
            select(Product)
            .options(
                selectinload(Product.images),
                selectinload(Product.features),
                selectinload(Product.attributes),
                selectinload(Product.videos),
                selectinload(Product.documents),
                selectinload(Product.category),
                selectinload(Product.industry),
                selectinload(Product.brand),
                selectinload(Product.product_type),
            )
            .offset(offset)
            .limit(batch_size)
        )

        products = result.scalars().all()
        if not products:
            break

        print(f"\n📦 Processing batch offset={offset}, size={len(products)}")

        actions = []

        for product in products:
            brand = product.brand.brand_name if product.brand else None
            product_type = (
                product.product_type.product_type if product.product_type else None
            )
            category = product.category.name if product.category else None

            attribute_values = [
                attr.attribute_value
                for attr in product.attributes
                if attr.attribute_value
            ]

            all_suggestions = build_suggestions_chain(
                brand=brand,
                product_type=product_type,
                category=category,
            )

            all_suggestions += [v for v in [product.mpn, product.sku] if v]
            all_suggestions = list(dict.fromkeys(all_suggestions))

            data = {
                "product_name": product.product_name,
                "sku": product.sku,
                "mpn": product.mpn,
                "gtin": product.gtin,
                "ean": product.ean,
                "upc": product.upc,
                "brand": brand,
                "product_type": product_type,
                "category": category,
                "category_name": category,
                "industry_name": (
                    product.industry.industry_name if product.industry else None
                ),
                "brand_suggest": brand,
                "product_type_suggest": product_type,
                "category_suggest": category,
                "mpn_suggest": product.mpn,
                "attribute_value_suggest": attribute_values,
                "all_suggestions": all_suggestions,
                "taxonomy": product.taxonomy,
                "vendor_name": product.vendor_name,
                "base_price": product.base_price,
                "sale_price": product.sale_price,
                "short_description": product.short_description,
                "long_description": product.long_description,
                "stock_qty": product.stock_qty,
                "features": [
                    {"name": f.name, "value": f.value} for f in product.features
                ],
                "attributes": [
                    {
                        "name": attr.attribute_name,
                        "value": attr.attribute_value,
                        "uom": attr.attribute_uom,
                    }
                    for attr in product.attributes
                ],
                "images": [
                    {"name": img.name, "url": img.url} for img in product.images
                ],
            }

            actions.append(
                {
                    "_op_type": "index",
                    "_id": product.id,
                    "_source": data,
                }
            )

        # -----------------------------
        # BULK INDEX using your service
        # -----------------------------
        success_count, errors = es_service.bulk(actions)

        batch_processed = len(products)
        batch_errors = len(errors) if errors else 0
        batch_created = success_count - batch_errors
        batch_updated = 0  # your service does not distinguish updates vs creates

        # Print some example errors
        if batch_errors:
            print("❌ Some errors in this batch (up to 5 shown):")
            for err in errors[:5]:
                print(err)

        print(
            f"📊 Batch Result: Processed={batch_processed}, "
            f"Created={batch_created}, Updated={batch_updated}, Errors={batch_errors}"
        )
        break

        total_indexed += len(products)
        offset += batch_size
        print(f"✅ Indexed so far: {total_indexed}")

    print(f"\n🔥 REINDEX COMPLETE: {total_indexed} products")
