# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.requests import Request
from app.core.config import settings
from app.api.routers import health
from app.api.routers import samples
from app.api.routers import data_collections
from app.api.routers import analysis_group

app = FastAPI(title="IGSR API")

# CORS (Settings expects JSON array in .env)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# IMPORTANT: no "/api" prefix here; nginx already strips "/api/" on proxy_pass
app.include_router(health.router)
app.include_router(samples.router)
app.include_router(data_collections.router)
app.include_router(analysis_group.router)

@app.get("/")
def root():
    return {"ok": True}

@app.exception_handler(Exception)
async def json_errors(request: Request, exc: Exception):
    # don’t leak internals; return a generic JSON error
    return JSONResponse(status_code=500, content={"error": "internal_server_error"})