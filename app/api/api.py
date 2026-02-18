from __future__ import annotations

from fastapi import APIRouter

from app.api.routers.cases import router as cases
from app.api.routers.documents import router as documents
from app.api.routers.transactions import router as transactions
from app.api.routers.notices import router as notices
from app.api.routers.files import router as files
from app.api.routers.audit import router as audit
from app.api.routers.tools import router as tools
from app.api.routers.dashboard import router as dashboard

api_router = APIRouter(prefix="/api")
api_router.include_router(cases)
api_router.include_router(documents)
api_router.include_router(transactions)
api_router.include_router(notices)
api_router.include_router(files)
api_router.include_router(audit)
api_router.include_router(tools)
api_router.include_router(dashboard)
