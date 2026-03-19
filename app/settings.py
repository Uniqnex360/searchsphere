from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    elastic_search_url: str
    debug: bool = False
    qdrant_url: str
    qdrant_api_key: str

    class Config:
        env_file = ".env"


settings = Settings()
