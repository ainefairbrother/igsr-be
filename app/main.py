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

tags_metadata = [
    {
        "name": "Analysis group",
        "description": "List all analysis groups.",
    },
    {
        "name": "Data collection",
        "description": "List all data collections.",
    },
    {
        "name": "File",
        "description": "List all files.",
    },
    {
        "name": "Population",
        "description": "List all populations or look up one population by ID.",
    },
    {
        "name": "Sample",
        "description": "List all samples or look up one sample by ID.",
    },
    {
        "name": "Superpopulation",
        "description": "List all superpopulations.",
    },
]

# Public base path the API is exposed under (e.g. /api behind the load balancer).
public_api_base = settings.API_BASE_PATH.rstrip("/") or "/"

app = FastAPI(
    title="IGSR API",
    version="2025",
    description="API for searching and retrieving IGSR sample, population, file, and collection data. Visit the main site at https://www.internationalgenome.org/ for more information about the project, or email info@1000genomes.org with questions or feedback.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1}
)
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
app.include_router(health.router, include_in_schema=False)
app.include_router(samples.router)
app.include_router(data_collections.router)
app.include_router(analysis_group.router)
app.include_router(population.router)
app.include_router(superpopulation.router)
app.include_router(file.router)
app.include_router(sitemap.router, include_in_schema=False)


@app.get("/", include_in_schema=False)
def root():
    return {"ok": True}


# return a generic JSON error instead of internal messages/logs and capture the trace in the log
@app.exception_handler(Exception)
async def json_errors(request: Request, exc: Exception):
    log.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"error": "internal_server_error"})
