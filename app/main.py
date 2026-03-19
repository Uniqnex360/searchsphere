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
    exists = any(c.name == "product" for c in collections)

    if not exists:
        qdrant_client.create_collection(
            collection_name=QDrantCollection.PRODUCT.value,
            vectors_config=models.VectorParams(
                size=384, distance=models.Distance.COSINE  # MiniLM output size
            ),
        )

    yield


app = FastAPI(title="Search Sphere", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for route in routers:
    app.include_router(route)


@app.get("/test-db")
async def test_db(session: AsyncSession = Depends(get_session)):
    print("database url", settings.database_url)
    try:
        result = await session.execute(text("SELECT 1"))
        return {"status": "success", "value": result.scalar()}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
