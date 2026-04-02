from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlmodel import select

from app.services import ElasticsearchService
from app.models import Product



async def sync_products_to_es(
    session: AsyncSession,
    es_service: ElasticsearchService,
    batch_size: int = 300,
):
    """
    Reindex all products into Elasticsearch in batches.
    Includes new fields: brand + product_type
    """

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

        print(f"📦 Processing batch offset={offset}, size={len(products)}")

        actions = []

        for product in products:
            data = {
                # search
                "product_name": product.product_name,
                # identifiers
                "sku": product.sku,
                "mpn": product.mpn,
                "gtin": product.gtin,
                "ean": product.ean,
                "upc": product.upc,
                # relations (🔥 updated)
                "brand": product.brand.brand_name if product.brand else None,
                "product_type": (
                    product.product_type.product_type if product.product_type else None
                ),
                "industry_name": (
                    product.industry.industry_name if product.industry else None
                ),
                "category": product.category.name if product.category else None,
                "category_name": product.category.name if product.category else None,
                # misc
                "taxonomy": product.taxonomy,
                "country_of_origin": product.country_of_origin,
                "warranty": product.warranty,
                # numeric
                "weight": product.weight,
                "length": product.length,
                "width": product.width,
                "height": product.height,
                "base_price": product.base_price,
                "sale_price": product.sale_price,
                "selling_price": product.selling_price,
                "special_price": product.special_price,
                # units
                "weight_unit": product.weight_unit,
                "dimension_unit": product.dimension_unit,
                "currency": product.currency,
                "stock_status": product.stock_status,
                # vendor
                "vendor_name": product.vendor_name,
                "vendor_sku": product.vendor_sku,
                # descriptions
                "short_description": product.short_description,
                "long_description": product.long_description,
                "meta_title": product.meta_title,
                "meta_description": product.meta_description,
                "search_keywords": product.search_keywords,
                # stock
                "stock_qty": product.stock_qty,
                # nested
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
                "videos": [
                    {"name": vid.name, "url": vid.url} for vid in product.videos
                ],
                "documents": [
                    {"name": doc.name, "url": doc.url} for doc in product.documents
                ],
            }

            actions.append(
                {
                    "_op_type": "index",  # or "update"
                    "_id": product.id,
                    "_source": data,
                }
            )

        # 🔥 BULK INSERT (IMPORTANT)
        es_service.bulk(actions)

        total_indexed += len(products)
        offset += batch_size

        print(f"✅ Indexed so far: {total_indexed}")

    print(f"\n🔥 REINDEX COMPLETE: {total_indexed} products")
