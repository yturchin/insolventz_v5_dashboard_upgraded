from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.paths import ensure_case_dirs
from app.repositories.case_repo import list_cases, get_case, create_case, update_case
from app.schemas.case import CaseCreate, CaseOut, CaseUpdate

router = APIRouter(prefix="/cases", tags=["cases"])


@router.get("", response_model=list[CaseOut])
def api_list_cases(db: Session = Depends(get_db)):
    return list_cases(db)


@router.post("", response_model=CaseOut)
def api_create_case(payload: CaseCreate, db: Session = Depends(get_db)):
    if get_case(db, payload.case_id):
        raise HTTPException(status_code=409, detail="case_id already exists")
    ensure_case_dirs(payload.case_id)
    c = create_case(
        db,
        payload.case_id,
        payload.company_name,
        [a.model_dump() for a in payload.accounts],
        court=payload.court,
        insolvenzantrag_date=payload.insolvenzantrag_date,
        eroeffnung_date=payload.eroeffnung_date,
        cutoff_date=payload.cutoff_date,
        metadata_json=payload.metadata_json,
    )
    return c


@router.get("/{case_id}", response_model=CaseOut)
def api_get_case(case_id: str, db: Session = Depends(get_db)):
    c = get_case(db, case_id)
    if not c:
        raise HTTPException(status_code=404, detail="Case not found")
    return c


@router.put("/{case_id}", response_model=CaseOut)
def api_update_case(case_id: str, payload: CaseUpdate, db: Session = Depends(get_db)):
    try:
        c = update_case(
            db,
            case_id,
            company_name=payload.company_name,
            court=payload.court,
            insolvenzantrag_date=payload.insolvenzantrag_date,
            eroeffnung_date=payload.eroeffnung_date,
            cutoff_date=payload.cutoff_date,
            metadata_json=payload.metadata_json,
            accounts=[a.model_dump() for a in payload.accounts] if payload.accounts is not None else None,
        )
        ensure_case_dirs(case_id)
        return c
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
