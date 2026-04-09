from typing import AsyncGenerator, Generator

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ============================================================
# 🔹 ASYNC SETUP (FastAPI)
# ============================================================

# Example: postgresql+asyncpg://user:pass@host/db
ASYNC_DATABASE_URL = settings.database_url

async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


# ============================================================
# 🔹 SYNC SETUP (Celery)
# ============================================================

# 🔥 Convert async URL → sync URL
# Example: postgresql+asyncpg:// → postgresql://
SYNC_DATABASE_URL = ASYNC_DATABASE_URL.replace("+asyncpg", "")

sync_engine = create_engine(
    SYNC_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    class_=Session,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


# 👉 Simple function (recommended for Celery)
def get_sync_session() -> Session:
    return SyncSessionLocal()


# 👉 Generator version (optional, FastAPI-style)
def get_sync_session_dep() -> Generator[Session, None, None]:
    session = SyncSessionLocal()
    try:
        yield session
    finally:
        session.close()
