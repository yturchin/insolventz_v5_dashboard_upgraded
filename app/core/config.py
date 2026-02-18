from __future__ import annotations

from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # root projects directory (relative to repo root)
    projects_dir: str = "projects"
    dataroom_dirname: str = "dataroom"
    db_filename: str = "insolventz_database.db"

    # OCR configuration (optional)
    tesseract_cmd: Optional[str] = None


settings = Settings()
