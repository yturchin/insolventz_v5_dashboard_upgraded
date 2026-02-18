from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.api import api_router
from app.db.init_db import init_db
from app.ui.router import ui_router


def create_app() -> FastAPI:
    init_db()
    app = FastAPI(title="Insolventz v4")

    app.include_router(api_router)

    # UI (server-rendered forensic dashboard)
    app.include_router(ui_router)

    static_dir = Path(__file__).resolve().parent / "static"
    # Keep static assets under /static
    app.mount("/static", StaticFiles(directory=str(static_dir), html=True), name="static")
    return app


app = create_app()
