from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional

class Settings(BaseSettings):
    PORT: int = 8000
    CORS_ALLOW_ORIGINS: List[str] = [""]
    API_BASE_PATH: str = "/api"

    ES_HOST: str = ""
    ES_CLOUD_ID: Optional[str] = None
    ES_USERNAME: str | None = None
    ES_PASSWORD: str | None = None
    ES_API_KEY: str | None = None

    INDEX_SAMPLE: str = "sample"
    INDEX_DATA_COLLECTIONS: str = "data_collections"  # FE path is /beta/data-collection/_search but the ES index is "data_collections"
    INDEX_ANALYSIS_GROUP: str = "analysis_group"
    INDEX_POPULATION: str = "population"
    INDEX_SUPERPOPULATION: str = "superpopulation"
    INDEX_FILE: str = "file"
    INDEX_SITEMAP: str = "sitemap"

    # When FE sends size:-1 ("all"), limit to this many hits in one request
    # This is because Elasticsearch’s size window is capped by the index setting max_result_window (which defaults to 10,000)
    # Asking for more than that in one request, ES will throw a 400
    # So, keep ≤ ES index max_result_window - here, I've set it to 100 as this is the max. that are displayed on the FE
    # at once anyway (as returned from /_search).
    ES_ALL_SIZE_CAP: int = 100  # used by normal /_search endpoints
    ES_EXPORT_SIZE_CAP: int = (
        10_000  # higher cap for file downloads (≤ index max_result_window)
    )

    # Allow .env file to override defaults
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


settings = Settings()
