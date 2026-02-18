from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.db.models import AuditEvent


def log_event(
    db: Session,
    *,
    case_id: Optional[str],
    action: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    actor: Optional[str] = "system",
    payload: Optional[Dict[str, Any]] = None,
) -> AuditEvent:
    ev = AuditEvent(
        case_id=case_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        actor=actor,
        payload=payload or {},
    )
    db.add(ev)
    db.flush()
    return ev
