from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    DateTime,
    Date,
    ForeignKey,
    Integer,
    String,
    Text,
    Float,
    UniqueConstraint,
    JSON,
    Boolean,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Case(Base):
    __tablename__ = "cases"

    # Stable human-readable ID (case_0001)
    case_id: Mapped[str] = mapped_column(String, primary_key=True)

    company_name: Mapped[str] = mapped_column(String, nullable=False)
    court: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Critical InsO dates (optional)
    insolvenzantrag_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    eroeffnung_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    cutoff_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Free-form metadata (UI settings, notes)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    accounts: Mapped[list["CompanyAccount"]] = relationship(
        "CompanyAccount", back_populates="case", cascade="all, delete-orphan"
    )
    documents: Mapped[list["Document"]] = relationship(
        "Document", back_populates="case", cascade="all, delete-orphan"
    )
    counterparties: Mapped[list["Counterparty"]] = relationship(
        "Counterparty", back_populates="case", cascade="all, delete-orphan"
    )
    company_details: Mapped[Optional["CompanyDetails"]] = relationship(
        "CompanyDetails", back_populates="case", uselist=False, cascade="all, delete-orphan"
    )


class CompanyAccount(Base):
    __tablename__ = "company_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)
    account_number: Mapped[str] = mapped_column(String, nullable=False)
    currency: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    case: Mapped[Case] = relationship("Case", back_populates="accounts")


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)
    document_type: Mapped[str] = mapped_column(String, nullable=False)
    file_name: Mapped[str] = mapped_column(String, nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)
    detected_format: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    processing_status: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # pending/processing/done/failed
    processing_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # OCR workflow
    ocr_progress: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=0)  # 0..100
    ocr_text_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    case: Mapped[Case] = relationship("Case", back_populates="documents")
    transactions: Mapped[list["Transaction"]] = relationship("Transaction", back_populates="document")


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)

    # Traceability / source
    source_document_id: Mapped[Optional[int]] = mapped_column(ForeignKey("documents.id"), nullable=True, index=True)
    source_file: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    import_batch_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Dates
    booking_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    value_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # Amount
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False, default="EUR")

    # Accounts / counterparties
    debtor_account_iban: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
    creditor_account_iban: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)

    creditor_name: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)
    debtor_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    purpose: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    end_to_end_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    bank_reference: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # v3-parity fields (raw inputs + linkage)
    counterparty_id: Mapped[Optional[int]] = mapped_column(ForeignKey("counterparties.id"), nullable=True, index=True)
    counterparty_name_raw: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    raw_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    booking_text: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    bic: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # v3 rule engine outputs
    rule_hits: Mapped[list] = mapped_column(JSON, default=list)

    # v4 legacy / UI-friendly aliases (kept for backward compatibility)
    source_account: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    transaction_date: Mapped[str] = mapped_column(String, nullable=False)
    recipient_account: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    recipient_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    transaction_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Verification + tagging
    verified_recipient_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tags: Mapped[str] = mapped_column(Text, default="[]")  # combined tags as JSON string (UI/backlog)
    system_tags: Mapped[list] = mapped_column(JSON, default=list)
    user_tags: Mapped[list] = mapped_column(JSON, default=list)
    user_tags_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Dedup
    tx_hash: Mapped[str] = mapped_column(String, index=True)

    is_duplicate: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    duplicate_of: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    dedup_cluster_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    document: Mapped[Optional["Document"]] = relationship("Document", back_populates="transactions")
    counterparty: Mapped[Optional["Counterparty"]] = relationship("Counterparty")


class DedupDecision(Base):
    __tablename__ = "dedup_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), index=True)

    decision: Mapped[str] = mapped_column(String, nullable=False)  # DUPLICATE / UNIQUE
    duplicate_of: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    method: Mapped[str] = mapped_column(String, default="exact_key_v2")
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    details: Mapped[dict] = mapped_column(JSON, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Counterparty(Base):
    __tablename__ = "counterparties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)

    name: Mapped[str] = mapped_column(String, nullable=False)
    account_number: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True)

    # classification: supplier/customer/shareholder/affiliate/other
    role: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # yes/no/unknown
    is_related_party: Mapped[str] = mapped_column(String, default="unknown")

    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    enrichment_json: Mapped[dict] = mapped_column(JSON, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    case: Mapped[Case] = relationship("Case", back_populates="counterparties")


class RuleEvaluation(Base):
    __tablename__ = "rule_evaluations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), index=True)

    rule_id: Mapped[str] = mapped_column(String, nullable=False)  # e.g. §130
    rule_version: Mapped[str] = mapped_column(String, default="1.0")
    decision: Mapped[str] = mapped_column(String, nullable=False)  # HIT/NO_HIT/NEEDS_REVIEW
    confidence: Mapped[float] = mapped_column(Float, default=0.0)

    explanation: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    legal_basis: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    lookback_start: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    lookback_end: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    conditions_met: Mapped[list] = mapped_column(JSON, default=list)
    conditions_missing: Mapped[list] = mapped_column(JSON, default=list)
    evidence_present: Mapped[list] = mapped_column(JSON, default=list)
    evidence_missing: Mapped[list] = mapped_column(JSON, default=list)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CompanyDetails(Base):
    __tablename__ = "company_details"  # renamed from Company Enrichment

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), unique=True, index=True)

    legal_name: Mapped[str] = mapped_column(String, nullable=True)
    legal_form: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    registered_address: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    hrb_number: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    register_court: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    management: Mapped[list] = mapped_column(JSON, default=list)
    shareholders: Mapped[list] = mapped_column(JSON, default=list)
    affiliates: Mapped[list] = mapped_column(JSON, default=list)

    enrichment_status: Mapped[str] = mapped_column(String, default="pending")
    enrichment_sources: Mapped[list] = mapped_column(JSON, default=list)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    case: Mapped[Case] = relationship("Case", back_populates="company_details")


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[Optional[str]] = mapped_column(String, index=True)

    actor: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # user/email or "system"
    action: Mapped[str] = mapped_column(String, nullable=False)  # e.g. "document.uploaded"

    entity_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # case/document/transaction/notice
    entity_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    payload: Mapped[dict] = mapped_column(JSON, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Notice(Base):
    __tablename__ = "notices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(ForeignKey("cases.case_id"), index=True)
    counterparty_name: Mapped[str] = mapped_column(String, nullable=False)

    document_name: Mapped[str] = mapped_column(String, nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)

    status: Mapped[str] = mapped_column(String, default="Generated")  # Generated | Accepted | Sent
    content: Mapped[str] = mapped_column(Text, default="")  # editable text

    # Optional: reference to transactions grouped into this notice
    transaction_ids: Mapped[list] = mapped_column(JSON, default=list)

    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)