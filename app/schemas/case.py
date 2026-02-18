from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class CompanyAccountIn(BaseModel):
    account_number: str
    currency: Optional[str] = None


class CompanyAccountOut(CompanyAccountIn):
    id: int


class CaseCreate(BaseModel):
    case_id: str = Field(..., pattern=r"^case_\d{4}$")
    company_name: str
    court: Optional[str] = None
    insolvenzantrag_date: Optional[date] = None
    eroeffnung_date: Optional[date] = None
    cutoff_date: Optional[date] = None
    metadata_json: Dict[str, Any] = {}
    accounts: List[CompanyAccountIn] = []


class CaseUpdate(BaseModel):
    company_name: Optional[str] = None
    court: Optional[str] = None
    insolvenzantrag_date: Optional[date] = None
    eroeffnung_date: Optional[date] = None
    cutoff_date: Optional[date] = None
    metadata_json: Optional[Dict[str, Any]] = None
    accounts: Optional[List[CompanyAccountIn]] = None


class CaseOut(BaseModel):
    case_id: str
    company_name: str
    court: Optional[str] = None
    insolvenzantrag_date: Optional[date] = None
    eroeffnung_date: Optional[date] = None
    cutoff_date: Optional[date] = None
    metadata_json: Dict[str, Any]
    accounts: List[CompanyAccountOut]

    class Config:
        from_attributes = True
