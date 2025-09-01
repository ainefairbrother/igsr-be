from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PORT: int = 8000
    CORS_ALLOW_ORIGINS: str = "http://localhost:8080"

    ES_HOST: str = "http://localhost:9200"
    ES_USERNAME: str | None = None
    ES_PASSWORD: str | None = None
    ES_API_KEY: str | None = None

    INDEX_SAMPLES: str = "sample"
    # FE path is /beta/data-collection/_search but the real index is "data_collections"
    INDEX_DATA_COLLECTIONS: str = "data_collections"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()