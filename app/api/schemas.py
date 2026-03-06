from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class HealthResponse(BaseModel):
    status: str = Field(
        ...,
        description=(
            "Service health status. 'ok' means the API can reach Elasticsearch; "
            "'degraded' means search requests may fail."
        ),
    )


class SearchRequest(BaseModel):
    """
    Common request body for search endpoints.
    """

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "query": {"match_all": {}},
                "size": 25,
                "sort": [{"name.keyword": "asc"}],
            }
        },
    )

    query: Dict[str, Any] = Field(
        default_factory=lambda: {"match_all": {}},
        description="Search query. If omitted, all records are matched.",
    )
    size: Optional[int] = Field(
        default=None,
        description=(
            "Number of records to return. Use -1 to request all records up to the API limit."
        ),
    )
    sort: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Sort order for returned records.",
    )
    source: Optional[Any] = Field(
        default=None,
        alias="_source",
        description=(
            "Optional field selection for returned records (for example ['name', 'sex'])."
        ),
    )
    track_total_hits: Optional[bool] = Field(
        default=None,
        description="Set to true to return the exact total count.",
    )


class ErrorDetailResponse(BaseModel):
    detail: str = Field(
        ...,
        description="Reason the request failed, in plain text or a short error code.",
    )

    model_config = ConfigDict(
        json_schema_extra={"example": {"detail": "backend_unavailable"}}
    )


class SourceDocument(BaseModel):
    """
    Generic wrapper for ES documents returned by detail endpoints.
    """

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "_source": {
                    "name": "HG00096",
                    "sex": "male",
                    "population": "GBR",
                }
            }
        },
    )

    source: Dict[str, Any] = Field(
        default_factory=dict,
        alias="_source",
        description="The returned record data.",
    )


class SearchHit(BaseModel):
    """
    One matching record in the search results.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: Optional[str] = Field(
        default=None,
        alias="_id",
        description="Unique record identifier.",
    )
    score: Optional[float] = Field(
        default=None,
        alias="_score",
        description="Search relevance score for this result.",
    )
    source: Dict[str, Any] = Field(
        default_factory=dict,
        alias="_source",
        description="The returned record data.",
    )


class SearchHits(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    total: int = Field(
        ...,
        description="Total number of matching records for the search.",
    )
    max_score: Optional[float] = Field(
        default=None,
        description="Highest relevance score in this result page.",
    )
    hits: List[SearchHit] = Field(
        default_factory=list,
        description="List of records returned for this page.",
    )


class SearchResponse(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "took": 6,
                "timed_out": False,
                "hits": {
                    "total": 2,
                    "max_score": 1.0,
                    "hits": [
                        {
                            "_id": "HG00096",
                            "_score": 1.0,
                            "_source": {"name": "HG00096", "sex": "male"},
                        }
                    ],
                },
                "aggregations": {},
            }
        },
    )

    took: int = Field(
        ...,
        description="How long the search took to run (milliseconds).",
    )
    timed_out: bool = Field(
        ...,
        description="True if the search timed out before completion.",
    )
    hits: SearchHits = Field(
        ...,
        description="Search result count and list of returned records.",
    )
    aggregations: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional grouped counts/facets requested by the query.",
    )
