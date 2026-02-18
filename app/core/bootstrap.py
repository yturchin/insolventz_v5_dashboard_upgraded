from __future__ import annotations

from .paths import dataroom_root, cases_root


def bootstrap_filesystem() -> None:
    dataroom_root().mkdir(parents=True, exist_ok=True)
    cases_root().mkdir(parents=True, exist_ok=True)
