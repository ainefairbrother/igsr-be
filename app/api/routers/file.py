# app/api/routers/file.py
"""
File router
=================
FE path: /api/beta/file/*  ->  here: /beta/file/*
"""

from fastapi import APIRouter, Body
from typing import Any, Dict, Optional
from app.core.config import settings

router = APIRouter(prefix="/beta/file", tags=["file"])
INDEX = settings.INDEX_FILE

# ------------------------------ Endpoints ------------------------------------

