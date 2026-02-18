from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.db.models import Case, CompanyAccount
from app.repositories.audit_repo import log_event


def list_cases(db: Session) -> List[Case]:
    return db.query(Case).order_by(Case.created_at.desc()).all()


def get_case(db: Session, case_id: str) -> Optional[Case]:
    return db.query(Case).filter(Case.case_id == case_id).first()


def create_case(
    db: Session,
    case_id: str,
    company_name: str,
    accounts: List[Dict[str, Any]],
    *,
    court: Optional[str] = None,
    insolvenzantrag_date=None,
    eroeffnung_date=None,
    cutoff_date=None,
    metadata_json: Optional[Dict[str, Any]] = None,
) -> Case:
    c = Case(
        case_id=case_id,
        company_name=company_name,
        court=court,
        insolvenzantrag_date=insolvenzantrag_date,
        eroeffnung_date=eroeffnung_date,
        cutoff_date=cutoff_date,
        metadata_json=metadata_json or {},
    )
    db.add(c)
    db.flush()

    for a in accounts:
        db.add(CompanyAccount(case_id=case_id, account_number=a["account_number"], currency=a.get("currency")))

    log_event(
        db,
        case_id=case_id,
        action="case.created",
        entity_type="case",
        entity_id=case_id,
        payload={"company_name": company_name, "accounts": accounts},
    )

    db.flush()
    db.refresh(c)
    return c


def replace_accounts(db: Session, case_id: str, accounts: List[Dict[str, Any]]) -> None:
    db.query(CompanyAccount).filter(CompanyAccount.case_id == case_id).delete()
    for a in accounts:
        db.add(CompanyAccount(case_id=case_id, account_number=a["account_number"], currency=a.get("currency")))


def update_case(
    db: Session,
    case_id: str,
    company_name: Optional[str] = None,
    accounts: Optional[List[Dict[str, Any]]] = None,
    court: Optional[str] = None,
    insolvenzantrag_date=None,
    eroeffnung_date=None,
    cutoff_date=None,
    metadata_json: Optional[Dict[str, Any]] = None,
) -> Case:
    c = get_case(db, case_id)
    if c is None:
        raise ValueError("Case not found")

    before = {
        "company_name": c.company_name,
        "court": c.court,
        "insolvenzantrag_date": str(c.insolvenzantrag_date) if c.insolvenzantrag_date else None,
        "eroeffnung_date": str(c.eroeffnung_date) if c.eroeffnung_date else None,
        "cutoff_date": str(c.cutoff_date) if c.cutoff_date else None,
        "metadata_json": c.metadata_json,
    }

    if company_name is not None:
        c.company_name = company_name
    if court is not None:
        c.court = court
    if insolvenzantrag_date is not None:
        c.insolvenzantrag_date = insolvenzantrag_date
    if eroeffnung_date is not None:
        c.eroeffnung_date = eroeffnung_date
    if cutoff_date is not None:
        c.cutoff_date = cutoff_date
    if metadata_json is not None:
        c.metadata_json = metadata_json

    if accounts is not None:
        replace_accounts(db, case_id, accounts)

    after = {
        "company_name": c.company_name,
        "court": c.court,
        "insolvenzantrag_date": str(c.insolvenzantrag_date) if c.insolvenzantrag_date else None,
        "eroeffnung_date": str(c.eroeffnung_date) if c.eroeffnung_date else None,
        "cutoff_date": str(c.cutoff_date) if c.cutoff_date else None,
        "metadata_json": c.metadata_json,
        "accounts": accounts if accounts is not None else None,
    }

    log_event(
        db,
        case_id=case_id,
        action="case.updated",
        entity_type="case",
        entity_id=case_id,
        payload={"before": before, "after": after},
    )

    db.flush()
    db.refresh(c)
    return c
