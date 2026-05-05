from fastapi import APIRouter, Depends, HTTPException
from elasticsearch import Elasticsearch, helpers

from app.services import ESCollection
from app.es_client import get_es
from app.helpers.embedding import get_embedding

router = APIRouter()


@router.post("/update-embedding/")
async def update_product_mapping(es: Elasticsearch = Depends(get_es)):
    index = ESCollection.PRODUCT_V6.value

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
# helper to build text
# -----------------------------
def build_text(doc: dict) -> str:
    return " ".join(
        filter(
            None,
            [
                doc.get("name", ""),
                doc.get("brand", ""),
                doc.get("category", ""),
                doc.get("product_type", ""),
                " ".join(
                    doc.get("attributes", [])
                    if isinstance(doc.get("attributes"), list)
                    else []
                ),
            ],
        )
    ).strip()


@router.post("/update-word-embeddings/")
async def update_word_embeddings(
    es: Elasticsearch = Depends(get_es),
    batch_size: int = 100,  # 👈 configurable batch size
):
    index = ESCollection.PRODUCT_V6.value

    if not es.indices.exists(index=index):
        raise HTTPException(status_code=404, detail="Index not found")

    try:
        scroll = es.search(
            index=index,
            scroll="2m",
            size=batch_size,
            body={
                "_source": [
                    "name",
                    "brand",
                    "category",
                    "product_type",
                    "attributes",
                ],
                "query": {"match_all": {}},
            },
        )

        scroll_id = scroll["_scroll_id"]
        hits = scroll["hits"]["hits"]

        total_updated = 0
        batch_count = 0

        while hits:
            actions = []

            for doc in hits:
                doc_id = doc["_id"]
                source = doc["_source"]

                text = build_text(source)
                embedding = get_embedding(text)

                if not embedding:
                    continue

                actions.append(
                    {
                        "_op_type": "update",
                        "_index": index,
                        "_id": doc_id,
                        "doc": {
                            "word_embedding": embedding,  # 👈 only new field
                        },
                    }
                )

            if actions:
                helpers.bulk(es, actions)
                total_updated += len(actions)
                batch_count += 1

                print(
                    f"Batch {batch_count} done | "
                    f"Updated: {len(actions)} docs | "
                    f"Total: {total_updated}"
                )

            # next batch
            scroll = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = scroll["_scroll_id"]
            hits = scroll["hits"]["hits"]

        print(f"✅ Completed | Total updated docs: {total_updated}")

        return {
            "message": "embedding backfill completed",
            "total_updated": total_updated,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
