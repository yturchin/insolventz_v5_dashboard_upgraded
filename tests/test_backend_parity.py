from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models import Case, CompanyAccount, Transaction
from app.services.ingest_service import compute_tx_hash
from app.services.dedup_service import run_dedup
from app.services.rules.rule_engine_service import evaluate_all
from app.repositories.counterparty_repo import get_or_create_counterparty


def _make_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def test_tx_hash_is_stable_and_matches_duplicates():
    h1 = compute_tx_hash(
        booking_date=date(2025, 1, 10),
        amount=-100.0,
        currency="EUR",
        debtor_iban="DE001234",
        creditor_iban="DE009999",
        creditor_name="ACME GmbH",
        purpose="Invoice 123",
        end_to_end_id="E2E-1",
    )
    h2 = compute_tx_hash(
        booking_date=date(2025, 1, 10),
        amount=-100.0001,
        currency="eur",
        debtor_iban="DE001234",
        creditor_iban="DE009999",
        creditor_name="Acme GmbH",
        purpose="Invoice 123",
        end_to_end_id="E2E-1",
    )
    # amount rounded to 2dp + normalized fields => same hash
    assert h1 == h2


def test_dedup_marks_overlap_across_sources():
    db = _make_session()
    case = Case(case_id="case_0001", company_name="TestCo", cutoff_date=date(2025, 2, 1))
    case.accounts.append(CompanyAccount(account_number="DE001234", currency="EUR"))
    db.add(case)
    db.flush()

    tx_hash = compute_tx_hash(
        booking_date=date(2025, 1, 10),
        amount=-100.0,
        currency="EUR",
        debtor_iban="DE001234",
        creditor_iban="DE009999",
        creditor_name="ACME GmbH",
        purpose="Invoice 123",
        end_to_end_id="E2E-1",
    )

    t1 = Transaction(
        case_id=case.case_id,
        booking_date=date(2025, 1, 10),
        amount=-100.0,
        currency="EUR",
        debtor_account_iban="DE001234",
        creditor_account_iban="DE009999",
        creditor_name="ACME GmbH",
        purpose="Invoice 123",
        end_to_end_id="E2E-1",
        source_file="/tmp/a.csv",
        tx_hash=tx_hash,
        transaction_date="2025-01-10",
        recipient_name="ACME GmbH",
        transaction_description="Invoice 123",
        tags="[]",
    )
    t2 = Transaction(
        case_id=case.case_id,
        booking_date=date(2025, 1, 10),
        amount=-100.0,
        currency="EUR",
        debtor_account_iban="DE001234",
        creditor_account_iban="DE009999",
        creditor_name="ACME GmbH",
        purpose="Invoice 123",
        end_to_end_id="E2E-1",
        source_file="/tmp/b.xlsx",
        tx_hash=tx_hash,
        transaction_date="2025-01-10",
        recipient_name="ACME GmbH",
        transaction_description="Invoice 123",
        tags="[]",
    )
    db.add_all([t1, t2])
    db.flush()

    stats = run_dedup(db, case_id=case.case_id)
    assert stats["duplicates"] == 1
    assert stats["canonical"] == 1

    db.refresh(t1)
    db.refresh(t2)
    assert (t1.is_duplicate and not t2.is_duplicate) or (t2.is_duplicate and not t1.is_duplicate)


def test_counterparty_fuzzy_match():
    db = _make_session()
    case = Case(case_id="case_0001", company_name="TestCo")
    db.add(case)
    db.flush()

    cp1 = get_or_create_counterparty(db, case_id=case.case_id, name="ACME GmbH", account_number=None)
    cp2 = get_or_create_counterparty(db, case_id=case.case_id, name="Acme Gesellschaft mit beschränkter Haftung", account_number=None)
    assert cp1.id == cp2.id


def test_rule_engine_evaluates_all_6_rules():
    db = _make_session()
    case = Case(case_id="case_0001", company_name="TestCo", cutoff_date=date(2025, 2, 1))
    db.add(case)
    db.flush()

    tx = Transaction(
        case_id=case.case_id,
        booking_date=date(2025, 1, 10),
        amount=-500.0,
        currency="EUR",
        transaction_date="2025-01-10",
        recipient_name="Creditor",
        transaction_description="Mahnung Ratenzahlung",
        tags="[]",
    )

    res = evaluate_all(tx, case, None)
    assert {r.rule_id for r in res} == {"§130", "§131", "§132", "§133", "§134", "§135"}
