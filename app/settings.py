from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    elastic_search_url: str
    elastic_search_api_key: str
    debug: bool = False
    qdrant_url: str
    qdrant_api_key: str
    gemini_api_key: str
    #shopify
    shopify_api_key: str
    shopify_api_secret: str
    shopify_scopes: str
    #server
    frontend_url: str
    backend_url: str

    class Config:
        env_file = ".env"


settings = Settings()
