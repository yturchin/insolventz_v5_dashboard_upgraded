from __future__ import annotations

import json
from typing import Any

from sqlalchemy import cast, func
from sqlalchemy.types import String
from sqlalchemy.orm import Session

from app.db.models import RuleEvaluation, Transaction


def _parse_list(qp, key: str) -> list[str]:
    vals = qp.getlist(key)
    out: list[str] = []
    for v in vals:
        if not v:
            continue
        for part in str(v).split(","):
            p = part.strip()
            if p:
                out.append(p)
    return out


def get_transactions_page(db: Session, case_id: str, qp) -> dict[str, Any]:
    """Forensic transactions list with advanced filters."""
    if not case_id:
        return {"rows": [], "total": 0, "page": 1, "page_size": 50, "filters": {}}

    page = int(qp.get("page", 1))
    page_size = int(qp.get("page_size", 50))
    page_size = max(10, min(page_size, 200))

    text = (qp.get("q") or "").strip()
    date_from = (qp.get("date_from") or "").strip()
    date_to = (qp.get("date_to") or "").strip()
    include_duplicates = (qp.get("include_duplicates") or "").lower() in ("1", "true", "yes", "on")

    rule_ids = _parse_list(qp, "rule_id")
    decisions = _parse_list(qp, "decision")
    system_tags = _parse_list(qp, "system_tag")
    user_tags = _parse_list(qp, "user_tag")

    q = db.query(Transaction).filter(Transaction.case_id == case_id)
    if not include_duplicates:
        q = q.filter(Transaction.is_duplicate == False)

    if text:
        like = f"%{text}%"
        q = q.filter(
            (Transaction.creditor_name.ilike(like))
            | (Transaction.recipient_name.ilike(like))
            | (Transaction.purpose.ilike(like))
            | (Transaction.transaction_description.ilike(like))
            | (Transaction.creditor_account_iban.ilike(like))
            | (Transaction.recipient_account.ilike(like))
        )
    if date_from:
        q = q.filter(Transaction.booking_date >= date_from)
    if date_to:
        q = q.filter(Transaction.booking_date <= date_to)

    # Filter by rule evaluations (exists)
    if rule_ids or decisions:
        sub = db.query(RuleEvaluation.transaction_id).filter(RuleEvaluation.case_id == case_id)
        if rule_ids:
            sub = sub.filter(RuleEvaluation.rule_id.in_(rule_ids))
        if decisions:
            sub = sub.filter(RuleEvaluation.decision.in_(decisions))
        q = q.filter(Transaction.id.in_(sub.subquery()))

    # system_tags/user_tags stored as JSON arrays; SQLite stores as TEXT; use LIKE fallback
    sys_txt = func.lower(cast(Transaction.system_tags, String))
    usr_txt = func.lower(cast(Transaction.user_tags, String))
    for t in system_tags:
        q = q.filter(sys_txt.like(f'%"{t.lower()}"%'))
    for t in user_tags:
        q = q.filter(usr_txt.like(f'%"{t.lower()}"%'))

    order = (qp.get("order") or "booking_date").strip()
    direction = (qp.get("dir") or "desc").strip().lower()
    col = getattr(Transaction, order, Transaction.booking_date)
    q = q.order_by(col.desc() if direction == "desc" else col.asc())

    total = q.count()
    rows = q.offset((page - 1) * page_size).limit(page_size).all()

    # Preload rule summaries for badge rendering
    rule_map = {}
    if rows:
        ids = [r.id for r in rows]
        rrows = (
            db.query(RuleEvaluation.transaction_id, RuleEvaluation.rule_id, RuleEvaluation.decision, RuleEvaluation.confidence)
            .filter(RuleEvaluation.case_id == case_id, RuleEvaluation.transaction_id.in_(ids))
            .all()
        )
        for tid, rid, dec, conf in rrows:
            rule_map.setdefault(tid, []).append({"rule_id": rid, "decision": dec, "confidence": float(conf or 0.0)})

    out_rows = []
    for r in rows:
        out_rows.append(
            {
                "id": r.id,
                "booking_date": r.booking_date.isoformat() if getattr(r.booking_date, "isoformat", None) else str(r.booking_date),
                "value_date": r.value_date.isoformat() if r.value_date else "",
                "amount": float(r.amount),
                "currency": r.currency,
                "counterparty": r.creditor_name or r.recipient_name or r.counterparty_name_raw or "(unknown)",
                "iban": r.creditor_account_iban or r.recipient_account or "",
                "purpose": (r.purpose or r.transaction_description or "")[:120],
                "is_duplicate": bool(r.is_duplicate),
                "tx_hash": r.tx_hash,
                "source_file": r.source_file or "",
                "system_tags": list(r.system_tags or []),
                "user_tags": list(r.user_tags or []),
                "rules": rule_map.get(r.id, []),
            }
        )

    return {
        "rows": out_rows,
        "total": int(total),
        "page": page,
        "page_size": page_size,
        "filters": {
            "q": text,
            "date_from": date_from,
            "date_to": date_to,
            "include_duplicates": include_duplicates,
            "rule_id": rule_ids,
            "decision": decisions,
            "system_tag": system_tags,
            "user_tag": user_tags,
            "order": order,
            "dir": direction,
        },
    }


def get_transaction_detail(db: Session, tx_id: int) -> dict[str, Any]:
    tx = db.query(Transaction).filter(Transaction.id == tx_id).first()
    if not tx:
        return {"tx": None, "rules": [], "dedup": None}

    rules = (
        db.query(RuleEvaluation)
        .filter(RuleEvaluation.case_id == tx.case_id, RuleEvaluation.transaction_id == tx.id)
        .order_by(RuleEvaluation.rule_id.asc())
        .all()
    )

    return {
        "tx": tx,
        "rules": rules,
    }
