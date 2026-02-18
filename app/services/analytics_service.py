from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from sqlalchemy import func, case, literal_column
from sqlalchemy.orm import Session

from app.db.models import Transaction, RuleEvaluation, Notice


DECISION_HIGH = "HIT"
DECISION_REVIEW = "NEEDS_REVIEW"
DECISION_LOW = "NO_HIT"


def _counterparty_expr():
    # For outgoing (amount < 0) creditor_name is usually the payee; for incoming use debtor_name.
    # Fallback to whichever is present.
    return func.coalesce(
        case((Transaction.amount < 0, Transaction.creditor_name), else_=Transaction.debtor_name),
        Transaction.creditor_name,
        Transaction.debtor_name,
        literal_column("'Unknown'"),
    )


def kpis(db: Session, case_id: str) -> Dict[str, Any]:
    total_inflows = db.query(func.coalesce(func.sum(Transaction.amount), 0.0))\
        .filter(Transaction.case_id == case_id, Transaction.amount > 0).scalar() or 0.0

    total_outflows = db.query(func.coalesce(func.sum(Transaction.amount), 0.0))\
        .filter(Transaction.case_id == case_id, Transaction.amount < 0).scalar() or 0.0

    suspicious_amount = db.query(func.coalesce(func.sum(func.abs(Transaction.amount)), 0.0))\
        .join(RuleEvaluation, RuleEvaluation.transaction_id == Transaction.id)\
        .filter(Transaction.case_id == case_id, RuleEvaluation.decision == DECISION_HIGH).scalar() or 0.0

    open_rule_hits = db.query(func.count(RuleEvaluation.id))\
        .filter(RuleEvaluation.case_id == case_id, RuleEvaluation.decision.in_([DECISION_HIGH, DECISION_REVIEW]))\
        .scalar() or 0

    notice_rows = db.query(Notice.status, func.count(Notice.id))\
        .filter(Notice.case_id == case_id)\
        .group_by(Notice.status).all()
    notice_counts = {status: int(cnt) for status, cnt in notice_rows}

    # coverage
    min_date, max_date = db.query(
        func.min(Transaction.booking_date),
        func.max(Transaction.booking_date),
    ).filter(Transaction.case_id == case_id).one()

    coverage = {
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None,
        "span_days": 0,
        "covered_days": 0,
        "missing_days": 0,
        "longest_gap_days": 0,
    }

    if min_date and max_date:
        span_days = int((max_date - min_date).days) + 1
        # distinct booking days
        covered_days = db.query(func.count(func.distinct(Transaction.booking_date)))\
            .filter(Transaction.case_id == case_id).scalar() or 0

        # gaps between consecutive distinct dates
        dates = [r[0] for r in db.query(func.distinct(Transaction.booking_date))\
            .filter(Transaction.case_id == case_id)\
            .order_by(Transaction.booking_date.asc()).all()]
        longest_gap = 0
        missing = 0
        for a, b in zip(dates, dates[1:]):
            gap = int((b - a).days) - 1
            if gap > 0:
                missing += gap
                longest_gap = max(longest_gap, gap)

        coverage.update(
            span_days=span_days,
            covered_days=int(covered_days),
            missing_days=int(missing),
            longest_gap_days=int(longest_gap),
        )

    # ratio (avoid division by zero)
    coverage_ratio = (coverage["covered_days"] / coverage["span_days"]) if coverage["span_days"] else 0.0

    return {
        "total_inflows": float(total_inflows),
        "total_outflows": float(total_outflows),  # negative
        "suspicious_amount": float(suspicious_amount),
        "open_rule_hits": int(open_rule_hits),
        "notice_counts": notice_counts,
        "coverage": coverage,
        "coverage_ratio": float(coverage_ratio),
    }


def monthly_cashflow(db: Session, case_id: str) -> List[Dict[str, Any]]:
    month = func.strftime("%Y-%m", Transaction.booking_date)
    rows = db.query(
        month.label("month"),
        func.sum(case((Transaction.amount > 0, Transaction.amount), else_=0.0)).label("inflows"),
        func.sum(case((Transaction.amount < 0, func.abs(Transaction.amount)), else_=0.0)).label("outflows"),
    ).filter(Transaction.case_id == case_id)\
     .group_by(month)\
     .order_by(month).all()

    return [{"month": m, "inflows": float(i or 0.0), "outflows": float(o or 0.0)} for m, i, o in rows]


def suspicious_trend(db: Session, case_id: str) -> List[Dict[str, Any]]:
    month = func.strftime("%Y-%m", Transaction.booking_date)
    rows = db.query(
        month.label("month"),
        func.sum(func.abs(Transaction.amount)).label("amount"),
    ).join(RuleEvaluation, RuleEvaluation.transaction_id == Transaction.id)\
     .filter(Transaction.case_id == case_id, RuleEvaluation.decision == DECISION_HIGH)\
     .group_by(month)\
     .order_by(month).all()
    return [{"month": m, "amount": float(a or 0.0)} for m, a in rows]


def risk_distribution(db: Session, case_id: str) -> Dict[str, int]:
    rows = db.query(RuleEvaluation.decision, func.count(RuleEvaluation.id))\
        .filter(RuleEvaluation.case_id == case_id)\
        .group_by(RuleEvaluation.decision).all()
    out = {"LOW": 0, "REVIEW": 0, "HIGH": 0}
    for decision, cnt in rows:
        if decision == DECISION_HIGH:
            out["HIGH"] += int(cnt)
        elif decision == DECISION_REVIEW:
            out["REVIEW"] += int(cnt)
        else:
            out["LOW"] += int(cnt)
    return out


def top_counterparties(db: Session, case_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    cp = _counterparty_expr()
    rows = db.query(
        cp.label("counterparty"),
        func.sum(func.abs(Transaction.amount)).label("volume"),
    ).filter(Transaction.case_id == case_id)\
     .group_by(cp)\
     .order_by(func.sum(func.abs(Transaction.amount)).desc())\
     .limit(limit).all()
    return [{"counterparty": c or "Unknown", "volume": float(v or 0.0)} for c, v in rows]


def high_risk_transactions(db: Session, case_id: str, limit: int = 25) -> List[Dict[str, Any]]:
    cp = _counterparty_expr()
    rows = db.query(
        Transaction.id,
        Transaction.booking_date,
        Transaction.amount,
        cp.label("counterparty"),
        RuleEvaluation.rule_id,
        RuleEvaluation.decision,
        RuleEvaluation.confidence,
    ).join(RuleEvaluation, RuleEvaluation.transaction_id == Transaction.id)\
     .filter(Transaction.case_id == case_id, RuleEvaluation.decision.in_([DECISION_HIGH, DECISION_REVIEW]))\
     .order_by(RuleEvaluation.confidence.desc(), func.abs(Transaction.amount).desc())\
     .limit(limit).all()

    out = []
    for tid, d, amt, cpn, rule_id, decision, conf in rows:
        out.append({
            "transaction_id": int(tid),
            "date": d.isoformat() if d else None,
            "amount": float(amt),
            "counterparty": cpn or "Unknown",
            "rule_id": rule_id,
            "severity": "HIGH" if decision == DECISION_HIGH else "REVIEW",
            "confidence": float(conf or 0.0),
        })
    return out


def notice_lifecycle(db: Session, case_id: str) -> Dict[str, int]:
    rows = db.query(Notice.status, func.count(Notice.id))\
        .filter(Notice.case_id == case_id)\
        .group_by(Notice.status).all()
    base = {"Draft": 0, "Generated": 0, "Accepted": 0, "Sent": 0}
    for status, cnt in rows:
        base[status] = int(cnt)
    return base
