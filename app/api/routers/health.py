"""
Health router
=============
"""

from typing import Dict
from fastapi import APIRouter
from app.services.es import es
from app.api.schemas import HealthResponse

router = APIRouter(prefix="/beta", tags=["health"])

# ------------------------------ Endpoints ------------------------------------


# curl -s -XGET http://localhost:8080/api/beta/health | jq
@router.get(
    "/health",
    summary="Service health check",
    response_model=HealthResponse,
    response_description="Reachability status for backing Elasticsearch cluster",
)
def health() -> Dict[str, str]:
    """
    GET /api/beta/health
    """
    try:
        ok = bool(es.ping())
    except Exception:
        ok = False
    return {"status": "ok" if ok else "degraded"}
