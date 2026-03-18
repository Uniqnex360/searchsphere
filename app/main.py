from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import text
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware

from app.database import get_session
from app.routes import routers
from app.settings import settings


app = FastAPI(title="Search Sphere")

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
