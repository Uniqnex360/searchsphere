"""
PUT products
{
  "mappings": {
    "properties": {
      "name": {
        "type": "search_as_you_type"
      }
    }
  }
}

GET products/_search
{
  "query": {
    "multi_match": {
      "query": "iph",
      "type": "bool_prefix",
      "fields": [
        "name",
        "name._2gram",
        "name._3gram"
      ],
      "fuzziness": "AUTO"
    }
  }
}
"""

from elasticsearch import Elasticsearch
from sqlalchemy.orm import selectinload
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from app.models import Product, Brand
from app.services import ElasticsearchService, ESCollection, ElasticsearchIndexManager


async def create_or_get_index_v6(es: Elasticsearch, index_name: str, index_type: str):
    """
    index_type: "autosuggest" | "product"
    """

    index_service = ElasticsearchIndexManager(es)

    # Common settings (reuse)
    settings = {
        "analysis": {
            "analyzer": {
                "lowercase_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                }
            }
        }
    }

    # Dynamic mapping based on type
    if index_type == ESCollection.PRODUCT_AUTO_SUGGEST_V6.value:
        mappings = {
            "properties": {
                "brand_name": {
                    "type": "search_as_you_type",
                    "analyzer": "lowercase_analyzer",
                },
                "brand_category": {
                    "type": "search_as_you_type",
                    "analyzer": "lowercase_analyzer",
                },
            }
        }

    elif index_type == ESCollection.PRODUCT_V6.value:
        mappings = {
            "properties": {
                "suggest": {
                    "type": "text",
                    "analyzer": "lowercase_analyzer",
                    "fields": {"keyword": {"type": "keyword"}},
                }
            }
        }

    else:
        raise ValueError("Invalid index_type")

    body = {"settings": settings, "mappings": mappings}

    index_service.ensure_index(index_name, body)

    return index_name


async def sync_product_suggest_data_es_v6(
    es: Elasticsearch, session: AsyncSession, batch_size: int = 100
) -> dict:
    """
    Sync product + autosuggest data using offset-based batching
    """

    autosuggest_index = ESCollection.PRODUCT_AUTO_SUGGEST_V6.value
    product_index = ESCollection.PRODUCT_V6.value

    autosuggest_service = ElasticsearchService(es, autosuggest_index)
    product_service = ElasticsearchService(es, product_index)

    # Ensure indices exist
    await create_or_get_index_v6(es, autosuggest_index, autosuggest_index)
    await create_or_get_index_v6(es, product_index, product_index)

    offset = 0
    total_processed = 0
    batch_number = 1

    print("🚀 Starting sync...")

    while True:
        print(f"\n📦 Fetching batch {batch_number} (offset={offset})")

        result = await session.execute(
            select(Product)
            .options(selectinload(Product.brand), selectinload(Product.category))
            .order_by(Product.id)  # ⚠️ IMPORTANT for consistent pagination
            .offset(offset)
            .limit(batch_size)
        )

        products = result.scalars().all()

        if not products:
            print("✅ No more data to process.")
            break

        autosuggest_actions = []
        product_actions = []

        for product in products:
            brand = product.brand
            category = product.category

            if not brand or not category or not category.name:
                continue

            new_entry = f"{brand.brand_name} {category.name}"

            autosuggest_actions.append(
                {
                    "_op_type": "update",
                    "_id": brand.id,
                    "script": {
                        "source": """
                    if (ctx._source.brand_category == null) {
                        ctx._source.brand_category = params.new_entries;
                    } else {
                        for (entry in params.new_entries) {
                            if (!ctx._source.brand_category.contains(entry)) {
                                ctx._source.brand_category.add(entry);
                            }
                        }
                    }
                """,
                        "params": {"new_entries": [new_entry]},
                    },
                    "upsert": {
                        "brand_name": brand.brand_name,
                        "brand_category": [new_entry],
                    },
                }
            )
            # -------- Product Doc --------
            product_actions.append(
                {
                    "_op_type": "index",
                    "_id": product.id,
                    "suggest": [brand.brand_name, new_entry],
                }
            )

        # Bulk insert
        auto_success, auto_errors = autosuggest_service.bulk(autosuggest_actions)
        prod_success, prod_errors = product_service.bulk(product_actions)

        print(f"✅ Autosuggest indexed: {auto_success}")
        print(f"✅ Product indexed: {prod_success}")

        if auto_errors:
            print(f"❌ Autosuggest errors: {len(auto_errors)}")

        if prod_errors:
            print(f"❌ Product errors: {len(prod_errors)}")

        total_processed += len(products)
        offset += batch_size
        batch_number += 1

        print(f"📊 Total processed so far: {total_processed}")

    print(f"\n🎉 Sync completed! Total records processed: {total_processed}")

    return {"total_processed": total_processed}
