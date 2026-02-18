from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AuditEvent


def get_audit_page(db: Session, case_id: str, qp) -> dict[str, Any]:
    actor = (qp.get("actor") or "").strip()
    action = (qp.get("action") or "").strip()
    entity_type = (qp.get("entity_type") or "").strip()

    q = db.query(AuditEvent)
    if case_id:
        q = q.filter(AuditEvent.case_id == case_id)
    if actor:
        q = q.filter(AuditEvent.actor.ilike(f"%{actor}%"))
    if action:
        q = q.filter(AuditEvent.action.ilike(f"%{action}%"))
    if entity_type:
        q = q.filter(AuditEvent.entity_type == entity_type)

    rows = q.order_by(AuditEvent.created_at.desc()).limit(500).all()

    return {
        "rows": rows,
        "filters": {"actor": actor, "action": action, "entity_type": entity_type},
    }
