from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import and_, case as sa_case, func
from sqlalchemy.orm import Session

from app.db.models import Notice, RuleEvaluation, Transaction


def get_overview_metrics(db: Session, case_id: str) -> dict:
    """High-level KPIs for forensic overview."""
    base = db.query(Transaction).filter(Transaction.case_id == case_id)

    total = base.count()
    canonical = base.filter(Transaction.is_duplicate == False).count()
    duplicates = base.filter(Transaction.is_duplicate == True).count()

    inflow = (
        db.query(func.coalesce(func.sum(Transaction.amount), 0.0))
        .filter(Transaction.case_id == case_id, Transaction.is_duplicate == False, Transaction.amount > 0)
        .scalar()
    )
    outflow = (
        db.query(func.coalesce(func.sum(Transaction.amount), 0.0))
        .filter(Transaction.case_id == case_id, Transaction.is_duplicate == False, Transaction.amount < 0)
        .scalar()
    )

    hit_count = (
        db.query(func.count(func.distinct(RuleEvaluation.transaction_id)))
        .filter(
            RuleEvaluation.case_id == case_id,
            RuleEvaluation.decision.in_(["HIT", "NEEDS_REVIEW"]),
        )
        .scalar()
    )
    high_conf = (
        db.query(func.count(func.distinct(RuleEvaluation.transaction_id)))
        .filter(
            RuleEvaluation.case_id == case_id,
            RuleEvaluation.decision == "HIT",
            RuleEvaluation.confidence >= 0.8,
        )
        .scalar()
    )

    suspicious_volume = (
        db.query(func.coalesce(func.sum(Transaction.amount), 0.0))
        .join(RuleEvaluation, RuleEvaluation.transaction_id == Transaction.id)
        .filter(
            Transaction.case_id == case_id,
            Transaction.is_duplicate == False,
            RuleEvaluation.decision.in_(["HIT", "NEEDS_REVIEW"]),
        )
        .scalar()
    )

    return {
        "total_transactions": int(total),
        "canonical_transactions": int(canonical),
        "duplicate_transactions": int(duplicates),
        "total_inflow": float(inflow or 0.0),
        "total_outflow": float(outflow or 0.0),
        "suspicious_volume": float(suspicious_volume or 0.0),
        "hit_transactions": int(hit_count or 0),
        "high_confidence_hits": int(high_conf or 0),
    }


def get_overview_timeseries(db: Session, case_id: str, days: int = 90) -> list[dict]:
    """Daily inflow/outflow series for the last N days."""
    # Determine window end as max booking_date if present, else today
    max_date = (
        db.query(func.max(Transaction.booking_date))
        .filter(Transaction.case_id == case_id)
        .scalar()
    )
    end = max_date or date.today()
    start = end - timedelta(days=days)

    rows = (
        db.query(
            Transaction.booking_date.label("d"),
            func.coalesce(func.sum(sa_case((Transaction.amount > 0, Transaction.amount), else_=0.0)), 0.0).label(
                "inflow"
            ),
            func.coalesce(func.sum(sa_case((Transaction.amount < 0, Transaction.amount), else_=0.0)), 0.0).label(
                "outflow"
            ),
        )
        .filter(
            Transaction.case_id == case_id,
            Transaction.is_duplicate == False,
            Transaction.booking_date >= start,
            Transaction.booking_date <= end,
        )
        .group_by(Transaction.booking_date)
        .order_by(Transaction.booking_date.asc())
        .all()
    )

    return [
        {
            "date": r.d.isoformat(),
            "inflow": float(r.inflow or 0.0),
            "outflow": float(r.outflow or 0.0),
        }
        for r in rows
    ]


def get_overview_rule_counts(db: Session, case_id: str) -> list[dict]:
    rows = (
        db.query(RuleEvaluation.rule_id, RuleEvaluation.decision, func.count(RuleEvaluation.id))
        .filter(RuleEvaluation.case_id == case_id)
        .group_by(RuleEvaluation.rule_id, RuleEvaluation.decision)
        .order_by(RuleEvaluation.rule_id.asc())
        .all()
    )
    out: dict[str, dict] = {}
    for rule_id, decision, cnt in rows:
        if rule_id not in out:
            out[rule_id] = {"rule_id": rule_id, "HIT": 0, "NEEDS_REVIEW": 0, "NO_HIT": 0}
        out[rule_id][decision] = int(cnt)
    return list(out.values())


def get_top_counterparties(db: Session, case_id: str, limit: int = 10) -> list[dict]:
    # Prefer counterparty_id if available; fall back to creditor_name/recipient_name
    name_expr = func.coalesce(Transaction.creditor_name, Transaction.recipient_name, Transaction.counterparty_name_raw)
    rows = (
        db.query(
            name_expr.label("name"),
            func.count(Transaction.id).label("tx_count"),
            func.coalesce(func.sum(Transaction.amount), 0.0).label("net_amount"),
            func.coalesce(func.sum(func.abs(Transaction.amount)), 0.0).label("gross_amount"),
        )
        .filter(Transaction.case_id == case_id, Transaction.is_duplicate == False)
        .group_by(name_expr)
        .order_by(func.sum(func.abs(Transaction.amount)).desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "name": r.name or "(unknown)",
            "tx_count": int(r.tx_count or 0),
            "net_amount": float(r.net_amount or 0.0),
            "gross_amount": float(r.gross_amount or 0.0),
        }
        for r in rows
    ]


def get_notice_status_counts(db: Session, case_id: str) -> dict:
    rows = (
        db.query(Notice.status, func.count(Notice.id))
        .filter(Notice.case_id == case_id)
        .group_by(Notice.status)
        .all()
    )
    out = {"Generated": 0, "Accepted": 0, "Sent": 0}
    for status, cnt in rows:
        out[str(status)] = int(cnt or 0)
    out["total"] = sum(out.values())
    return out


def get_statement_coverage(db: Session, case_id: str) -> dict:
    """Estimate date coverage based on booking_date presence."""
    min_d = db.query(func.min(Transaction.booking_date)).filter(Transaction.case_id == case_id).scalar()
    max_d = db.query(func.max(Transaction.booking_date)).filter(Transaction.case_id == case_id).scalar()
    if not min_d or not max_d:
        return {"min_date": None, "max_date": None, "span_days": 0, "covered_days": 0, "coverage_pct": 0.0, "missing_days": 0, "longest_gap_days": 0}

    span_days = (max_d - min_d).days + 1
    covered_days = (
        db.query(func.count(func.distinct(Transaction.booking_date)))
        .filter(Transaction.case_id == case_id, Transaction.is_duplicate == False)
        .scalar()
    ) or 0

    # Find gaps: days with no canonical transactions
    # Strategy: build set of dates present (works fine for case sizes typical in UI)
    dates = [
        r[0]
        for r in db.query(func.distinct(Transaction.booking_date))
        .filter(Transaction.case_id == case_id, Transaction.is_duplicate == False)
        .order_by(Transaction.booking_date.asc())
        .all()
        if r and r[0]
    ]
    if not dates:
        return {"min_date": min_d.isoformat(), "max_date": max_d.isoformat(), "span_days": span_days, "covered_days": 0, "coverage_pct": 0.0, "missing_days": span_days, "longest_gap_days": span_days}

    present = set(dates)
    missing_days = 0
    longest_gap = 0
    current_gap = 0
    d = min_d
    from datetime import timedelta as _td
    while d <= max_d:
        if d in present:
            longest_gap = max(longest_gap, current_gap)
            current_gap = 0
        else:
            missing_days += 1
            current_gap += 1
        d += _td(days=1)
    longest_gap = max(longest_gap, current_gap)

    coverage_pct = (covered_days / span_days * 100.0) if span_days else 0.0
    return {
        "min_date": min_d.isoformat(),
        "max_date": max_d.isoformat(),
        "span_days": int(span_days),
        "covered_days": int(covered_days),
        "coverage_pct": float(coverage_pct),
        "missing_days": int(missing_days),
        "longest_gap_days": int(longest_gap),
    }


def get_high_risk_transactions(db: Session, case_id: str, limit: int = 15) -> list[dict]:
    """Top transactions with rule hits/reviews, prioritizing abs(amount) and confidence."""
    name_expr = func.coalesce(Transaction.creditor_name, Transaction.recipient_name, Transaction.counterparty_name_raw)

    rows = (
        db.query(
            Transaction.id.label("tx_id"),
            Transaction.booking_date.label("booking_date"),
            Transaction.amount.label("amount"),
            Transaction.currency.label("currency"),
            name_expr.label("counterparty"),
            func.max(RuleEvaluation.confidence).label("max_conf"),
            func.count(RuleEvaluation.id).label("rule_hits"),
        )
        .join(RuleEvaluation, RuleEvaluation.transaction_id == Transaction.id)
        .filter(
            Transaction.case_id == case_id,
            Transaction.is_duplicate == False,
            RuleEvaluation.case_id == case_id,
            RuleEvaluation.decision.in_(["HIT", "NEEDS_REVIEW"]),
        )
        .group_by(Transaction.id, Transaction.booking_date, Transaction.amount, Transaction.currency, name_expr)
        .order_by(func.abs(Transaction.amount).desc(), func.max(RuleEvaluation.confidence).desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "tx_id": int(r.tx_id),
            "booking_date": r.booking_date.isoformat() if r.booking_date else None,
            "counterparty": r.counterparty or "(unknown)",
            "amount": float(r.amount or 0.0),
            "currency": r.currency or "",
            "rule_hits": int(r.rule_hits or 0),
            "max_conf": float(r.max_conf or 0.0),
        }
        for r in rows
    ]
