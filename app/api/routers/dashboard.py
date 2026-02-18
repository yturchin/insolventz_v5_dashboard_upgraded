from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.services.analytics_service import (
    kpis,
    monthly_cashflow,
    suspicious_trend,
    risk_distribution,
    top_counterparties,
    high_risk_transactions,
    notice_lifecycle,
)

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard/{case_id}")
def get_dashboard(case_id: str, db: Session = Depends(get_db)):
    return {
        "kpis": kpis(db, case_id),
        "monthly_cashflow": monthly_cashflow(db, case_id),
        "suspicious_trend": suspicious_trend(db, case_id),
        "risk_distribution": risk_distribution(db, case_id),
        "top_counterparties": top_counterparties(db, case_id),
        "high_risk": high_risk_transactions(db, case_id),
        "notice_lifecycle": notice_lifecycle(db, case_id),
    }
