from __future__ import annotations

from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Counterparty, RuleEvaluation, Transaction


def get_counterparties_page(db: Session, case_id: str, qp) -> dict[str, Any]:
    qtxt = (qp.get("q") or "").strip()

    q = (
        db.query(
            Counterparty.id,
            Counterparty.name,
            Counterparty.account_number,
            Counterparty.role,
            Counterparty.is_related_party,
            func.count(Transaction.id).label("tx_count"),
            func.coalesce(func.sum(Transaction.amount), 0.0).label("net_amount"),
            func.coalesce(func.sum(func.abs(Transaction.amount)), 0.0).label("gross_amount"),
            func.count(func.distinct(RuleEvaluation.rule_id)).label("rules_hit"),
        )
        .outerjoin(Transaction, (Transaction.counterparty_id == Counterparty.id) & (Transaction.is_duplicate == False))
        .outerjoin(
            RuleEvaluation,
            (RuleEvaluation.transaction_id == Transaction.id)
            & (RuleEvaluation.decision.in_(["HIT", "NEEDS_REVIEW"])),
        )
        .filter(Counterparty.case_id == case_id)
        .group_by(Counterparty.id)
        .order_by(func.sum(func.abs(Transaction.amount)).desc().nullslast())
    )
    if qtxt:
        q = q.filter(Counterparty.name.ilike(f"%{qtxt}%"))

    rows = q.limit(500).all()

    out = []
    for r in rows:
        out.append(
            {
                "id": int(r.id),
                "name": r.name,
                "account_number": r.account_number or "",
                "role": r.role or "",
                "is_related_party": r.is_related_party,
                "tx_count": int(r.tx_count or 0),
                "net_amount": float(r.net_amount or 0.0),
                "gross_amount": float(r.gross_amount or 0.0),
                "rules_hit": int(r.rules_hit or 0),
            }
        )

    return {"rows": out, "filters": {"q": qtxt}}
