from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PORT: int = 8000
    CORS_ALLOW_ORIGINS: str = "http://localhost:8080"

    ES_HOST: str = "http://localhost:9200"
    ES_USERNAME: str | None = None
    ES_PASSWORD: str | None = None
    ES_API_KEY: str | None = None

    INDEX_SAMPLE: str = "sample"
    INDEX_DATA_COLLECTIONS: str = "data_collections" # FE path is /beta/data-collection/_search but the ES index is "data_collections"
    INDEX_ANALYSIS_GROUP: str = "analysis_group"
    INDEX_POPULATION: str = "population"
    
    # When FE sends size:-1 ("all"), limit to this many hits in one request
    # This is because Elasticsearch’s size window is capped by the index setting max_result_window (which defaults to 10,000)
    # Asking for more than that in one request, ES will throw a 400
    # So, keep ≤ ES index max_result_window (defaults to 10_000)
    ES_ALL_SIZE_CAP: int = 10_000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()