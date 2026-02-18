"""Dedup service (v3 parity).

Marks overlapping transactions as duplicates across multiple source files.
Does NOT delete rows; sets flags + writes decision log + audit.

Key: booking_date + amount(2dp) + currency + debtor_iban + creditor_iban + counterparty + description.
"""

from __future__ import annotations

from typing import Dict, Tuple

from sqlalchemy.orm import Session

from app.db.models import Transaction, DedupDecision
from app.repositories.audit_repo import log_event


def _dedup_key(tx: Transaction) -> Tuple:
    return (
        str(tx.booking_date) if tx.booking_date else None,
        round(float(tx.amount or 0.0), 2),
        (tx.currency or "EUR").upper(),
        (tx.debtor_account_iban or "").strip().upper(),
        (tx.creditor_account_iban or "").strip().upper(),
        (tx.counterparty_name_raw or tx.creditor_name or tx.recipient_name or "").strip().upper()[:50],
        (tx.raw_description or tx.normalized_description or tx.purpose or tx.transaction_description or "").strip()[:80],
    )


def run_dedup(db: Session, *, case_id: str) -> Dict:
    txs = (
        db.query(Transaction)
        .filter(Transaction.case_id == case_id, Transaction.is_duplicate == False)
        .order_by(Transaction.booking_date, Transaction.created_at)
        .all()
    )

    seen: Dict[Tuple, int] = {}
    duplicates_found = 0

    for tx in txs:
        key = _dedup_key(tx)
        if key in seen:
            canonical_id = seen[key]
            tx.is_duplicate = True
            tx.duplicate_of = canonical_id
            tx.dedup_cluster_id = canonical_id

            db.add(
                DedupDecision(
                    case_id=case_id,
                    transaction_id=tx.id,
                    decision="DUPLICATE",
                    duplicate_of=canonical_id,
                    method="exact_key_v2",
                    confidence=1.0,
                    reason="Exact key match (date+amount+currency+iban+counterparty+desc)",
                    details={"key": [str(k) for k in key]},
                )
            )

            log_event(
                db,
                case_id=case_id,
                action="DEDUP_MARK_DUPLICATE",
                entity_type="transaction",
                entity_id=str(tx.id),
                payload={"duplicate_of": canonical_id, "method": "exact_key_v2"},
            )

            duplicates_found += 1
        else:
            seen[key] = tx.id

    db.flush()
    return {
        "total_checked": len(txs),
        "duplicates_found": duplicates_found,
        "unique": len(txs) - duplicates_found,
        # backward compatible keys used by earlier pipeline UI/logs
        "duplicates": duplicates_found,
        "canonical": len(txs) - duplicates_found,
    }
