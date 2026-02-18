from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.db.models import Case, Transaction, Counterparty, RuleEvaluation
from app.services.dedup_service import run_dedup
from app.services.rules.rule_engine_service import evaluate_all
from app.repositories.audit_repo import log_event

router = APIRouter(prefix="/tools", tags=["tools"])


@router.post("/dedup")
def api_run_dedup(case_id: str, db: Session = Depends(get_db)):
    c = db.query(Case).filter(Case.case_id == case_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Case not found")
    stats = run_dedup(db, case_id=case_id)
    log_event(db, case_id=case_id, action="dedup.run", entity_type="case", entity_id=case_id, payload=stats)
    db.commit()
    return stats


@router.post("/evaluate")
def api_run_evaluation(case_id: str, db: Session = Depends(get_db)):
    c = db.query(Case).filter(Case.case_id == case_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Case not found")

    db.query(RuleEvaluation).filter(RuleEvaluation.case_id == case_id).delete()

    txs = db.query(Transaction).filter(Transaction.case_id == case_id, Transaction.is_duplicate == False).all()
    evaluated = 0

    for tx in txs:
        cp = None
        if tx.counterparty_id:
            cp = db.query(Counterparty).filter(Counterparty.id == tx.counterparty_id).first()
        results = evaluate_all(tx, c, cp)
        for r in results:
            db.add(
                RuleEvaluation(
                    case_id=case_id,
                    transaction_id=tx.id,
                    rule_id=r.rule_id,
                    rule_version=r.rule_version,
                    decision=r.decision,
                    confidence=r.confidence,
                    explanation=r.explanation,
                    legal_basis=r.legal_basis,
                    lookback_start=r.lookback_start,
                    lookback_end=r.lookback_end,
                    conditions_met=r.conditions_met or [],
                    conditions_missing=r.conditions_missing or [],
                    evidence_present=r.evidence_present or [],
                    evidence_missing=r.evidence_missing or [],
                )
            )
        evaluated += 1

    log_event(db, case_id=case_id, action="rules.evaluate_all", entity_type="case", entity_id=case_id, payload={"evaluated": evaluated})
    db.commit()
    return {"evaluated": evaluated}
