from elasticsearch import Elasticsearch
from app.services import ESCollection


async def update_product_view_count(
    es: Elasticsearch, product: dict, debug: bool = False
):

    try:

        def log(msg):
            if debug:
                print(f"[ES VIEW COUNT] {msg}")

        brand = product.get("brand")
        sku = product.get("sku")
        mpn = product.get("mpn")

        log(f"Incoming product: {product}")

        must_clauses = []

        # -------------------------
        # SAFE FILTER BUILDING
        # -------------------------
        if sku:
            must_clauses.append({"term": {"sku.keyword": sku}})
            log(f"sku filter: {sku}")

        if mpn:
            must_clauses.append({"term": {"mpn.keyword": mpn}})
            log(f"mpn filter: {mpn}")

        if brand:
            must_clauses.append({"term": {"brand.keyword": brand}})
            log(f"brand filter: {brand}")

        if not must_clauses:
            log("No filters provided — aborting update")
            return

        query = {"bool": {"must": must_clauses}}

        log(f"Final ES query: {query}")

        resp = es.search(
            index=ESCollection.PRODUCT_V7.value, body={"query": query}, size=1
        )

        hits = resp.get("hits", {}).get("hits", [])

        log(f"Hits found: {len(hits)}")

        if not hits:
            log("No matching document found")
            return

        doc_id = hits[0]["_id"]
        log(f"Matched doc_id: {doc_id}")

        # -------------------------
        # DYNAMIC FIELD UPDATE
        # -------------------------
        es.update(
            index=ESCollection.PRODUCT_V7.value,
            id=doc_id,
            body={
                "script": {
                    "lang": "painless",
                    "source": """
                        if (ctx._source.containsKey('view_count')) {
                            ctx._source.view_count += 1;
                        } else {
                            ctx._source.view_count = 1;
                        }
                    """,
                }
            },
        )

        log("View count updated successfully")

    except Exception as e:
        print("ES view count update failed:", str(e))


def increment_search_popularity(
    es: Elasticsearch,
    index: str,
    product_ids: list[str],
):
    if not product_ids:
        return

    actions = []

    for pid in product_ids:
        actions.append({"update": {"_index": index, "_id": pid}})
        actions.append(
            {
                "script": {
                    "source": """
                    if (ctx._source.search_popularity == null) {
                        ctx._source.search_popularity = 0;
                    }
                    ctx._source.search_popularity += 1;
                """
                }
            }
        )

    es.bulk(body=actions)
