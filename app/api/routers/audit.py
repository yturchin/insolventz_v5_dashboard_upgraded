from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.db.models import AuditEvent

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("")
def list_audit(case_id: Optional[str] = None, limit: int = 200, db: Session = Depends(get_db)):
    q = db.query(AuditEvent)
    if case_id:
        q = q.filter(AuditEvent.case_id == case_id)
    items = q.order_by(AuditEvent.created_at.desc()).limit(min(limit, 1000)).all()
    return [
        {
            "id": it.id,
            "case_id": it.case_id,
            "actor": it.actor,
            "action": it.action,
            "entity_type": it.entity_type,
            "entity_id": it.entity_id,
            "payload": it.payload,
            "created_at": it.created_at.isoformat(),
        }
        for it in items
    ]
