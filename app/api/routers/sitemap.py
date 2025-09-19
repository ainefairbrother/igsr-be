# app/api/routers/sitemap.py
from typing import Any, Dict, Optional, List, Callable
from fastapi import APIRouter, Body, Query

from app.core.config import settings
from app.lib.search_utils import run_search

router = APIRouter(prefix="/sitemap", tags=["sitemap"])
