"""
Health router
=============
"""

from typing import Dict
from fastapi import APIRouter
from app.services.es import es

router = APIRouter(prefix="/beta", tags=["health"])

# ------------------------------ Endpoints ------------------------------------


# curl -s -XGET http://localhost:8080/api/beta/health | jq
@router.get("/health")
def health() -> Dict[str, str]:
    """
    GET /api/beta/health
    """
    try:
        ok = bool(es.ping())
    except Exception:
        ok = False
    return {"status": "ok" if ok else "degraded"}
