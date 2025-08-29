# app/routers/health.py
from fastapi import APIRouter
from app.services.es import es

router = APIRouter()

@router.get("/health")
def health():
    # ES ping is fastest; fall back to cluster health if needed
    ok = es.ping()
    return {"status": "ok" if ok else "degraded"}