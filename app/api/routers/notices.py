from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.db.models import Transaction
from app.repositories.case_repo import get_case
from app.repositories.notice_repo import (
    create_notice,
    list_notices,
    get_notice,
    update_notice_content,
    update_notice_status,
)
from app.repositories.audit_repo import log_event
from app.schemas.notice import NoticeOut, NoticeUpdate, NoticeStatusUpdate
from app.services.notice_service import (
    make_notice_filename,
    notice_path,
    render_notice_pdf,
    default_notice_text,
)

router = APIRouter(prefix="/notices", tags=["notices"])


class GenerateNoticesIn(BaseModel):
    case_id: str
    transaction_ids: list[int]


@router.get("", response_model=list[NoticeOut])
def api_list_notices(case_id: str, db: Session = Depends(get_db)):
    return list_notices(db, case_id)


@router.post("/generate", response_model=list[NoticeOut])
def api_generate_notices(payload: GenerateNoticesIn, db: Session = Depends(get_db)):
    c = get_case(db, payload.case_id)
    if not c:
        raise HTTPException(status_code=404, detail="Case not found")

    txs = (
        db.query(Transaction)
        .filter(Transaction.case_id == payload.case_id, Transaction.id.in_(payload.transaction_ids))
        .all()
    )
    if not txs:
        return []

    # group by counterparty (recipient_name preferred)
    groups: dict[str, list[Transaction]] = {}
    for tx in txs:
        key = (tx.recipient_name or "").strip() or (tx.recipient_account or "Unknown")
        groups.setdefault(key, []).append(tx)

    created = []
    for counterparty, items in groups.items():
        lines = []
        for it in sorted(items, key=lambda t: t.transaction_date):
            lines.append(
                f"{it.transaction_date} | {it.amount:.2f} {it.currency or ''} | {(it.transaction_description or '')}".strip()
            )

        content = default_notice_text(c.company_name, counterparty, lines)
        filename = make_notice_filename(counterparty, doc_type="notice")
        pdf_path = notice_path(payload.case_id, filename)
        render_notice_pdf(payload.case_id, pdf_path, content)
        n = create_notice(
            db,
            payload.case_id,
            counterparty,
            filename,
            str(pdf_path),
            content,
            transaction_ids=[it.id for it in items],
        )
        created.append(n)

    return created


@router.get("/{notice_id}", response_model=NoticeOut)
def api_get_notice(notice_id: int, db: Session = Depends(get_db)):
    n = get_notice(db, notice_id)
    if not n:
        raise HTTPException(status_code=404, detail="Notice not found")
    return n


@router.put("/{notice_id}", response_model=NoticeOut)
def api_update_notice(notice_id: int, payload: NoticeUpdate, db: Session = Depends(get_db)):
    try:
        n = update_notice_content(db, notice_id, payload.content)
        return n
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{notice_id}/render_pdf", response_model=NoticeOut)
def api_render_notice_pdf(notice_id: int, db: Session = Depends(get_db)):
    n = get_notice(db, notice_id)
    if not n:
        raise HTTPException(status_code=404, detail="Notice not found")

    render_notice_pdf(n.case_id, Path(n.file_path), n.content)
    log_event(db, case_id=n.case_id, action="notice.pdf_rendered", entity_type="notice", entity_id=str(n.id), payload={"file_path": n.file_path})
    return n


@router.patch("/{notice_id}/status", response_model=NoticeOut)
def api_update_notice_status(notice_id: int, payload: NoticeStatusUpdate, db: Session = Depends(get_db)):
    if payload.status not in {"Generated", "Accepted", "Sent"}:
        raise HTTPException(status_code=400, detail="Invalid status")
    try:
        return update_notice_status(db, notice_id, payload.status)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
