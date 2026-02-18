from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.repositories.notice_repo import list_notices, get_notice


def get_notices_page(db: Session, case_id: str, qp) -> dict[str, Any]:
    notices = list_notices(db, case_id)
    # basic counts by status for UI quick filters
    counts = {"Generated": 0, "Accepted": 0, "Sent": 0, "total": 0}
    for n in notices:
        counts[str(n.status)] = counts.get(str(n.status), 0) + 1
        counts["total"] += 1

    status_filter = (qp.get("status") or "").strip()
    if status_filter:
        notices = [n for n in notices if str(n.status) == status_filter]

    return {"notices": notices, "counts": counts, "filters": {"status": status_filter}}


def get_notice_detail(db: Session, notice_id: int) -> dict[str, Any]:
    n = get_notice(db, notice_id)
    return {"notice": n}
