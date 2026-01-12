from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class HealthResponse(BaseModel):
    status: str = Field(..., description="Overall API health status")


class SourceDocument(BaseModel):
    """
    Generic wrapper for ES documents returned by detail endpoints.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    source: Dict[str, Any] = Field(
        default_factory=dict,
        alias="_source",
        description="Document payload as stored in Elasticsearch",
    )


class SearchHit(BaseModel):
    """
    Minimal ES hit representation; extra fields (highlight, sort, etc.)
    are permitted for flexibility.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: Optional[str] = Field(
        default=None, alias="_id", description="Elasticsearch document id"
    )
    score: Optional[float] = Field(
        default=None, alias="_score", description="Elasticsearch relevance score"
    )
    source: Dict[str, Any] = Field(
        default_factory=dict,
        alias="_source",
        description="Document payload as stored in Elasticsearch",
    )


class SearchHits(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    total: int = Field(..., description="Total number of hits for the query")
    max_score: Optional[float] = Field(
        default=None, description="Maximum score across hits"
    )
    hits: List[SearchHit] = Field(default_factory=list, description="Matching hits")


class SearchResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    took: int = Field(..., description="Elasticsearch execution time in ms")
    timed_out: bool = Field(..., description="Whether the search timed out")
    hits: SearchHits
    aggregations: Dict[str, Any] = Field(
        default_factory=dict, description="Aggregation results, if requested"
    )
