from __future__ import annotations

"""Enrichment services extracted as separate module.

Real integrations (Handelsregister, VIES, Bundesanzeiger, Google Search) are intentionally
abstracted behind providers.

This v4.1 implementation provides:
- CompanyDetails enrichment skeleton (status tracking, sources)
- Counterparty enrichment skeleton

You can plug in providers later without touching ingestion logic.
"""

from dataclasses import dataclass
from typing import Optional, Protocol

from sqlalchemy.orm import Session

from app.db.models import Case, CompanyDetails, Counterparty
from app.repositories.audit_repo import log_event


class CompanyProvider(Protocol):
    def enrich(self, company_name: str) -> dict:
        ...


@dataclass
class DummyProvider:
    """Offline-safe provider used by default."""

    name: str = "dummy"

    def enrich(self, company_name: str) -> dict:
        return {
            "legal_name": company_name,
            "legal_form": None,
            "registered_address": None,
            "hrb_number": None,
            "register_court": None,
            "management": [],
            "shareholders": [],
            "affiliates": [],
            "sources": ["manual"],
        }


def ensure_company_details(db: Session, case: Case) -> CompanyDetails:
    cd = db.query(CompanyDetails).filter(CompanyDetails.case_id == case.case_id).first()
    if cd:
        return cd
    cd = CompanyDetails(case_id=case.case_id, legal_name=case.company_name, enrichment_status="pending")
    db.add(cd)
    db.flush()
    return cd


def enrich_company_details(db: Session, case_id: str, provider: Optional[CompanyProvider] = None) -> CompanyDetails:
    provider = provider or DummyProvider()
    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise ValueError("Case not found")

    cd = ensure_company_details(db, case)
    cd.enrichment_status = "running"
    db.flush()

    data = provider.enrich(case.company_name)

    cd.legal_name = data.get("legal_name") or cd.legal_name
    cd.legal_form = data.get("legal_form")
    cd.registered_address = data.get("registered_address")
    cd.hrb_number = data.get("hrb_number")
    cd.register_court = data.get("register_court")
    cd.management = data.get("management") or []
    cd.shareholders = data.get("shareholders") or []
    cd.affiliates = data.get("affiliates") or []

    cd.enrichment_sources = list({*(cd.enrichment_sources or []), *(data.get("sources") or [provider.__class__.__name__])})
    cd.enrichment_status = "completed"

    log_event(
        db,
        case_id=case_id,
        action="company_details.enriched",
        entity_type="company_details",
        entity_id=str(cd.id),
        payload={"provider": provider.__class__.__name__, "sources": cd.enrichment_sources},
    )

    db.flush()
    db.refresh(cd)
    return cd


def enrich_counterparty(db: Session, counterparty_id: int) -> Counterparty:
    cp = db.query(Counterparty).filter(Counterparty.id == counterparty_id).first()
    if not cp:
        raise ValueError("Counterparty not found")

    # Stub: mark enrichment attempted. Plug providers later.
    cp.enrichment_json = {**(cp.enrichment_json or {}), "status": "completed", "sources": ["manual"]}

    log_event(
        db,
        case_id=cp.case_id,
        action="counterparty.enriched",
        entity_type="counterparty",
        entity_id=str(cp.id),
        payload={"sources": cp.enrichment_json.get("sources")},
    )

    db.flush()
    db.refresh(cp)
    return cp
