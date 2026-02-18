from __future__ import annotations

from pathlib import Path
from .config import settings


def repo_root() -> Path:
    # assumes app/ is at repo root/app
    return Path(__file__).resolve().parents[2]


def projects_root() -> Path:
    return repo_root() / settings.projects_dir


def dataroom_root() -> Path:
    return projects_root() / settings.dataroom_dirname


def cases_root() -> Path:
    return dataroom_root() / "cases"


def db_path() -> Path:
    return dataroom_root() / settings.db_filename


def case_dir(case_id: str) -> Path:
    return cases_root() / case_id


def ensure_case_dirs(case_id: str) -> None:
    base = case_dir(case_id)
    (base / "source_info" / "bank_statements").mkdir(parents=True, exist_ok=True)
    (base / "source_info" / "list_of_creditors").mkdir(parents=True, exist_ok=True)
    (base / "notices").mkdir(parents=True, exist_ok=True)
    (base / "clawnotice").mkdir(parents=True, exist_ok=True)
    (base / "reports").mkdir(parents=True, exist_ok=True)
