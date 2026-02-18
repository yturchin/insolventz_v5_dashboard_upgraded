from __future__ import annotations

from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import RuleEvaluation, Transaction


def get_rules_review_page(db: Session, case_id: str, qp) -> dict[str, Any]:
    rule_id = (qp.get("rule_id") or "").strip() or None
    decision = (qp.get("decision") or "").strip() or None
    min_conf = qp.get("min_conf")
    try:
        min_conf_f = float(min_conf) if min_conf not in (None, "") else None
    except Exception:
        min_conf_f = None

    q = (
        db.query(RuleEvaluation, Transaction)
        .join(Transaction, Transaction.id == RuleEvaluation.transaction_id)
        .filter(RuleEvaluation.case_id == case_id, Transaction.is_duplicate == False)
    )
    if rule_id:
        q = q.filter(RuleEvaluation.rule_id == rule_id)
    if decision:
        q = q.filter(RuleEvaluation.decision == decision)
    if min_conf_f is not None:
        q = q.filter(RuleEvaluation.confidence >= min_conf_f)

    q = q.order_by(RuleEvaluation.decision.asc(), RuleEvaluation.confidence.desc(), Transaction.booking_date.desc())

    rows = q.limit(500).all()

    # Summary counts
    summary = (
        db.query(RuleEvaluation.rule_id, RuleEvaluation.decision, func.count(RuleEvaluation.id))
        .filter(RuleEvaluation.case_id == case_id)
        .group_by(RuleEvaluation.rule_id, RuleEvaluation.decision)
        .order_by(RuleEvaluation.rule_id.asc())
        .all()
    )
    summary_map: dict[str, dict] = {}
    for rid, dec, cnt in summary:
        summary_map.setdefault(rid, {"rule_id": rid, "HIT": 0, "NEEDS_REVIEW": 0, "NO_HIT": 0})
        summary_map[rid][dec] = int(cnt)

    return {
        "rows": rows,
        "filters": {"rule_id": rule_id or "", "decision": decision or "", "min_conf": min_conf or ""},
        "summary": list(summary_map.values()),
    }
