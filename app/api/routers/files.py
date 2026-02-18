from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.core.paths import dataroom_root

router = APIRouter(prefix="/files", tags=["files"])


def _is_under(root: Path, p: Path) -> bool:
    try:
        p.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


@router.get("/download")
def download(path: str):
    p = Path(path)
    root = dataroom_root()
    if not _is_under(root, p):
        raise HTTPException(status_code=403, detail="Forbidden path")
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(p))
