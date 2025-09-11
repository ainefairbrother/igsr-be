# app/api/routers/health.py
"""
Health router
=============
FE path: /api/beta/health  ->  here: /beta/health
"""

from typing import Dict
from fastapi import APIRouter
from app.services.es import es

router = APIRouter(prefix="/beta", tags=["health"])

# curl -s -XGET http://localhost:8080/api/beta/health | jq
@router.get("/health")
def health() -> Dict[str, str]:
    try:
        ok = bool(es.ping())
    except Exception:
        ok = False
    return {"status": "ok" if ok else "degraded"}