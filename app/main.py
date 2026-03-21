from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import text
from qdrant_client import models
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware

from app.database import get_session
from app.routes import routers
from app.settings import settings
from app.services import get_qdrant_client, QDrantCollection


@asynccontextmanager
async def lifespan(app: FastAPI):
    qdrant_client = get_qdrant_client()

    collections = qdrant_client.get_collections().collections
    exists = any(c.name == QDrantCollection.PRODUCT.value for c in collections)

    # Create the collection if missing
    if not exists:
        qdrant_client.create_collection(
            collection_name=QDrantCollection.PRODUCT.value,
            vectors_config=models.VectorParams(
                size=384,
                distance=models.Distance.COSINE,
            ),
        )

    # Get payload schema to see existing indexed fields
    info = qdrant_client.get_collection(QDrantCollection.PRODUCT.value)
    existing_payload_schema = info.payload_schema or {}

    # List of fields to index
    index_fields = ["brand", "category_name"]

    for field in index_fields:
        if field not in existing_payload_schema:
            print(f"Creating payload index for field: {field}")
            qdrant_client.create_payload_index(
                collection_name=QDrantCollection.PRODUCT.value,
                field_name=field,
                field_schema="keyword",  # simple string type
            )

    yield


app = FastAPI(title="Search Sphere")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://35.175.188.201.nip.io",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for route in routers:
    app.include_router(route)


@app.get("/test-db")
async def test_db(session: AsyncSession = Depends(get_session)):
    try:
        result = await session.execute(text("SELECT 1"))
        return {"status": "success", "value": result.scalar()}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
