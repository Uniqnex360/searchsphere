from qdrant_client import QdrantClient
from app.settings import settings

_qdrant_client = None


def get_qdrant_client() -> QdrantClient:
    global _qdrant_client

    if _qdrant_client is None:
        _qdrant_client = QdrantClient(
            url=settings.qdrant_url,
            api_key=settings.qdrant_api_key,
        )

    return _qdrant_client
