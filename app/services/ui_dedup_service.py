from __future__ import annotations

from typing import Any

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from app.db.models import Transaction


def get_dedup_clusters_page(db: Session, case_id: str, qp) -> dict[str, Any]:
    if not case_id:
        return {"clusters": []}

    # clusters where there is at least one duplicate
    rows = (
        db.query(
            Transaction.dedup_cluster_id.label("cluster_id"),
            func.count(Transaction.id).label("cnt"),
            func.sum(case((Transaction.is_duplicate == True, 1), else_=0)).label("dup_cnt"),
            func.max(Transaction.booking_date).label("last_date"),
        )
        .filter(Transaction.case_id == case_id, Transaction.dedup_cluster_id.isnot(None))
        .group_by(Transaction.dedup_cluster_id)
        .having(func.sum(case((Transaction.is_duplicate == True, 1), else_=0)) > 0)
        .order_by(func.max(Transaction.booking_date).desc())
        .limit(500)
        .all()
    )

    clusters = []
    for r in rows:
        canonical = (
            db.query(Transaction)
            .filter(
                Transaction.case_id == case_id,
                Transaction.dedup_cluster_id == r.cluster_id,
                Transaction.is_duplicate == False,
            )
            .order_by(Transaction.id.asc())
            .first()
        )
        clusters.append(
            {
                "cluster_id": int(r.cluster_id),
                "count": int(r.cnt),
                "duplicates": int(r.dup_cnt),
                "last_date": r.last_date.isoformat() if r.last_date else "",
                "canonical": canonical,
            }
        )

    return {"clusters": clusters}
