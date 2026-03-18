# alembic/env.py
from logging.config import fileConfig
from sqlalchemy import create_engine, pool
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel
from alembic import context

from app.settings import settings

# env.py
from app.models.industry import Industry
from app.models.category import Category
from app.models.product import (
    Product,
    ProductImage,
    ProductVideo,
    ProductDocument,
    ProductFeature,
    ProductAttribute,
)

print("SQLModel.metadata.tables.keys():", SQLModel.metadata.tables.keys())

# Alembic Config object
config = context.config

# Set up logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# SQLModel metadata for autogenerate
target_metadata = SQLModel.metadata


# -----------------------------
# Offline migrations
# -----------------------------
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = settings.database_url  # use .env URL
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


# -----------------------------
# Online migrations
# -----------------------------
def run_migrations_online() -> None:
    """Run migrations in 'online' mode (synchronous engine)."""
    # Alembic cannot use asyncpg directly, so replace asyncpg with psycopg2
    sync_url = settings.database_url.replace(
        "postgresql+asyncpg", "postgresql+psycopg2"
    )

    # create a synchronous engine
    connectable = create_engine(sync_url, poolclass=pool.NullPool, echo=True)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


# -----------------------------
# Run appropriate mode
# -----------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
