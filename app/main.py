import logging
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.requests import Request

from app.core.config import settings
from app.api.routers import health
from app.api.routers import samples
from app.api.routers import data_collections
from app.api.routers import analysis_group
from app.api.routers import population
from app.api.routers import superpopulation
from app.api.routers import file
from app.api.routers import sitemap

app = FastAPI(title="IGSR API")
log = logging.getLogger("uvicorn.error")

@app.middleware("http")
async def add_api_marker(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["x-igsr-api"] = "Python FastAPI"
    resp.headers["x-igsr-api-version"] = "2025"
    return resp

# CORS (Settings expects JSON array in .env)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all routers
app.include_router(health.router)
app.include_router(samples.router)
app.include_router(data_collections.router)
app.include_router(analysis_group.router)
app.include_router(population.router)
app.include_router(superpopulation.router)
app.include_router(file.router)
app.include_router(sitemap.router)

@app.get("/")
def root():
    return {"ok": True}

# return a generic JSON error instead of internal messages/logs and capture the trace in the log
@app.exception_handler(Exception)
async def json_errors(request: Request, exc: Exception):
    log.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"error": "internal_server_error"})