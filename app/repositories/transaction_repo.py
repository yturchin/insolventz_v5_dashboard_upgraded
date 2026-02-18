from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session

from app.db.models import Transaction
from app.repositories.audit_repo import log_event


def _tags_to_db(tags: List[str]) -> str:
    return json.dumps(sorted(set(tags)))


def _tags_from_db(tags: str) -> List[str]:
    try:
        return json.loads(tags or "[]")
    except Exception:
        return []


def create_transactions(db: Session, tx_rows: List[Dict]) -> Tuple[int, int]:
    """Insert transactions; silently skip duplicates based on uq_case_tx_hash.
    Returns (inserted, skipped).

    Note: the richer v4.1 ingestion path is `pipeline_service.process_document`.
    """

    inserted = 0
    skipped = 0
    for r in tx_rows:
        tx = Transaction(
            case_id=r["case_id"],
            source_account=r.get("source_account"),
            currency=r.get("currency"),
            transaction_date=r["transaction_date"],
            recipient_account=r.get("recipient_account"),
            recipient_name=r.get("recipient_name"),
            transaction_description=r.get("transaction_description"),
            amount=float(r["amount"]),
            verified_recipient_id=r.get("verified_recipient_id"),
            tags=r.get("tags") if isinstance(r.get("tags"), str) else _tags_to_db(r.get("tags", [])),
            source_file=r.get("source_file"),
            tx_hash=r["tx_hash"],
            counterparty_id=r.get("counterparty_id"),
        )
        db.add(tx)
        try:
            db.flush()
            inserted += 1
        except Exception:
            db.rollback()
            skipped += 1

    return inserted, skipped


def list_transactions(
    db: Session,
    case_id: str,
    recipient_name: Optional[str] = None,
    transaction_description: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    tags_any: Optional[List[str]] = None,
    order_by: str = "transaction_date",
    order_dir: str = "desc",
) -> List[Transaction]:
    # v3 parity: list only non-duplicate transactions by default
    q = db.query(Transaction).filter(Transaction.case_id == case_id, Transaction.is_duplicate == False)

    if recipient_name:
        q = q.filter(Transaction.recipient_name.ilike(f"%{recipient_name}%"))
    if transaction_description:
        q = q.filter(Transaction.transaction_description.ilike(f"%{transaction_description}%"))
    if date_from:
        q = q.filter(Transaction.transaction_date >= date_from)
    if date_to:
        q = q.filter(Transaction.transaction_date <= date_to)

    if tags_any:
        for t in tags_any:
            q = q.filter(Transaction.tags.ilike(f"%\"{t}\"%"))

    col = getattr(Transaction, order_by, Transaction.transaction_date)
    q = q.order_by(col.desc() if order_dir.lower() == "desc" else col.asc())

    return q.all()


def update_transaction_tags(db: Session, tx_id: int, tags: List[str]) -> Transaction:
    tx = db.query(Transaction).filter(Transaction.id == tx_id).first()
    if tx is None:
        raise ValueError("Transaction not found")

    before = _tags_from_db(tx.tags)
    tx.tags = _tags_to_db(tags)

    log_event(
        db,
        case_id=tx.case_id,
        action="transaction.tags_updated",
        entity_type="transaction",
        entity_id=str(tx.id),
        payload={"before": before, "after": _tags_from_db(tx.tags)},
    )

    db.flush()
    db.refresh(tx)
    return tx


def to_out(tx: Transaction) -> dict:
    return {
        "id": tx.id,
        "case_id": tx.case_id,
        "source_account": tx.source_account,
        "currency": tx.currency,
        "transaction_date": tx.transaction_date,
        "recipient_account": tx.recipient_account,
        "recipient_name": tx.recipient_name,
        "transaction_description": tx.transaction_description,
        "amount": tx.amount,
        "verified_recipient_id": tx.verified_recipient_id,
        "tags": _tags_from_db(tx.tags),
        "system_tags": list(tx.system_tags or []),
        "rule_hits": list(tx.rule_hits or []),
        "source_file": tx.source_file,
        "is_duplicate": bool(tx.is_duplicate),
        "duplicate_of": tx.duplicate_of,
    }
