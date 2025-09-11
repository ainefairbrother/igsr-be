# app/routers/health.py
"""
Health router
=======================
"""

from fastapi import APIRouter
from app.services.es import es

router = APIRouter()

# Health check endpoint
# curl -s -XGET http://localhost:8080/api/beta/health | jq
@router.get("/health")
def health():
    # ES ping is fastest; fall back to cluster health if needed
    ok = es.ping()
    return {"status": "ok" if ok else "degraded"}