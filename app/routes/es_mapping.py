from fastapi import APIRouter, Depends, HTTPException
from elasticsearch import Elasticsearch, helpers

from app.services import ESCollection
from app.es_client import get_es
from app.helpers.embedding import get_embedding

router = APIRouter()


@router.post("/update-embedding/")
async def update_product_mapping(es: Elasticsearch = Depends(get_es)):
    index = ESCollection.PRODUCT_V7.value

    if not es.indices.exists(index=index):
        raise HTTPException(status_code=404, detail="Index not found")

    mapping = es.indices.get_mapping(index=index)
    props = mapping[index]["mappings"].get("properties", {})

    # if "embedding" in props:
    #     return {"message": "embedding already exists"}
    if "word_embedding" in props:
        return {"message": "word embedding already exists"}

    try:
        es.indices.put_mapping(
            index=index,
            properties={
                "word_embedding": {
                    "type": "dense_vector",
                    "dims": 384,
                }
            },
        )

        return {"message": "embedding field added successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# FIXED helper to build text
# -----------------------------
def build_text(doc: dict) -> str:
    # Use product_name as per your mapping, fallback to name
    name = doc.get("product_name") or doc.get("name") or ""
    brand = doc.get("brand") or ""
    category = doc.get("category") or ""
    p_type = doc.get("product_type") or ""

    parts = [str(name), str(brand), str(category), str(p_type)]

    # Handle attributes list of dicts
    attrs = doc.get("attributes", [])
    if isinstance(attrs, list):
        for a in attrs:
            if isinstance(a, dict):
                v = a.get("value")
                if v:
                    parts.append(str(v))
            elif isinstance(a, str):
                parts.append(a)

    return " ".join(filter(None, parts)).strip()


@router.post("/update-word-embeddings/")
async def update_word_embeddings(
    es: Elasticsearch = Depends(get_es),
    batch_size: int = 100,
):
    index = ESCollection.PRODUCT_V7.value  # Ensure this is the correct index name

    if not es.indices.exists(index=index):
        raise HTTPException(status_code=404, detail="Index not found")

    def document_generator(hits):
        for doc in hits:
            try:
                source = doc.get("_source", {})
                text = build_text(source)

                if not text:
                    continue

                embedding = get_embedding(text)

                if embedding:
                    yield {
                        "_op_type": "update",
                        "_index": index,
                        "_id": doc["_id"],
                        "doc": {
                            "word_embedding": embedding,
                        },
                    }
            except Exception as e:
                print(f"Error processing doc {doc.get('_id')}: {e}")
                continue

    try:
        scroll_timeout = "5m"
        # Adjusted _source to match your actual mapping field names
        page = es.search(
            index=index,
            scroll=scroll_timeout,
            size=batch_size,
            body={
                "_source": [
                    "product_name",
                    "name",
                    "brand",
                    "category",
                    "product_type",
                    "attributes",
                ],
                "query": {"match_all": {}},
            },
        )

        scroll_id = page["_scroll_id"]
        hits = page["hits"]["hits"]
        total_updated = 0

        while hits:
            actions = list(document_generator(hits))
            if actions:
                success, _ = helpers.bulk(es, actions, stats_only=True)
                total_updated += success
                print(f"Updated: {success} | Total: {total_updated}")

            page = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
            scroll_id = page["_scroll_id"]
            hits = page["hits"]["hits"]

        es.clear_scroll(scroll_id=scroll_id)
        return {"status": "success", "total_updated": total_updated}

    except Exception as e:
        print(f"Backfill failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run/es_code/")
async def run_elastic_search(es: Elasticsearch = Depends(get_es)):
    try:
        response = es.indices.put_settings(
            index="product_v7", body={"index": {"max_result_window": 200000}}
        )

        return {"acknowledged": response.get("acknowledged", False)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
