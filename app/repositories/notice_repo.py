from __future__ import annotations

from typing import List, Optional

from sqlalchemy.orm import Session

from app.db.models import Notice
from app.repositories.audit_repo import log_event


def create_notice(
    db: Session,
    case_id: str,
    counterparty_name: str,
    document_name: str,
    file_path: str,
    content: str,
    transaction_ids: Optional[List[int]] = None,
) -> Notice:
    n = Notice(
        case_id=case_id,
        counterparty_name=counterparty_name,
        document_name=document_name,
        file_path=file_path,
        content=content,
        status="Generated",
        transaction_ids=transaction_ids or [],
    )
    db.add(n)
    db.flush()

    log_event(
        db,
        case_id=case_id,
        action="notice.generated",
        entity_type="notice",
        entity_id=str(n.id),
        payload={"counterparty": counterparty_name, "document_name": document_name, "transaction_ids": transaction_ids or []},
    )

    db.flush()
    db.refresh(n)
    return n


def list_notices(db: Session, case_id: str) -> List[Notice]:
    return db.query(Notice).filter(Notice.case_id == case_id).order_by(Notice.updated_at.desc()).all()


def get_notice(db: Session, notice_id: int) -> Optional[Notice]:
    return db.query(Notice).filter(Notice.id == notice_id).first()


def update_notice_content(db: Session, notice_id: int, content: str) -> Notice:
    n = get_notice(db, notice_id)
    if n is None:
        raise ValueError("Notice not found")

    before = (n.content or "")[:2000]
    n.content = content

    log_event(
        db,
        case_id=n.case_id,
        action="notice.edited",
        entity_type="notice",
        entity_id=str(n.id),
        payload={"before_preview": before, "after_preview": (content or "")[:2000]},
    )

    db.flush()
    db.refresh(n)
    return n


def update_notice_status(db: Session, notice_id: int, status: str) -> Notice:
    n = get_notice(db, notice_id)
    if n is None:
        raise ValueError("Notice not found")

    before = n.status
    n.status = status

    log_event(
        db,
        case_id=n.case_id,
        action="notice.status_changed",
        entity_type="notice",
        entity_id=str(n.id),
        payload={"before": before, "after": status},
    )

    db.flush()
    db.refresh(n)
    return n
