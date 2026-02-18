from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from sqlalchemy.orm import Session

from app.core.paths import case_dir, ensure_case_dirs
from app.repositories.case_repo import create_case, get_case
from app.repositories.document_repo import create_document
from app.services.pipeline_service import process_document


@dataclass
class SeedResult:
    case_id: str
    documents: list[int]
    inserted: int
    evaluated: int


def _make_statement_df(*, account_iban: str, start: date, n: int = 40) -> pd.DataFrame:
    """Generate a synthetic bank statement dataframe that reliably triggers:
    - inflow/outflow
    - repeated counterparties
    - duplicated rows (to test dedup)
    - descriptions suitable for rule engine heuristics
    """

    rows = []
    for i in range(n):
        d = start + timedelta(days=i)

        # Alternate inflow/outflow; create a few "suspicious" patterns
        if i % 7 == 0:
            amount = -2500.00
            cp = "Consulting Partner GmbH"
            purpose = "Beratungsleistung / kurzfristig"
            cp_iban = "DE02120300000000202051"
        elif i % 11 == 0:
            amount = -9800.00
            cp = "Related Party AG"
            purpose = "Darlehen Rueckzahlung / Gesellschafter"
            cp_iban = "DE75512108001245126199"
        elif i % 5 == 0:
            amount = 12000.00
            cp = "Key Customer Sp. z o.o."
            purpose = "Invoice 2026-{:03d}".format(i)
            cp_iban = "PL61109010140000071219812874"
        else:
            amount = -150.00 * (1 + (i % 3))
            cp = "Office Supplies GmbH"
            purpose = "Buerobedarf / Rechnung"
            cp_iban = "DE89370400440532013000"

        rows.append(
            {
                "booking_date": d.isoformat(),
                "value_date": d.isoformat(),
                "amount": amount,
                "currency": "EUR",
                "source_account": account_iban,
                "recipient_name": cp,
                "recipient_account": cp_iban,
                "transaction_description": purpose,
                "end_to_end_id": f"E2E{i:06d}",
                "bank_reference": f"BR{i:06d}",
            }
        )

    df = pd.DataFrame(rows)
    # Create a few exact duplicates (cross-file and same-file)
    if len(df) >= 5:
        df = pd.concat([df, df.iloc[[3, 10, 17]]], ignore_index=True)
    return df


def _write_pdf_text(path: Path, df: pd.DataFrame) -> None:
    """Write a simple text-based PDF statement; parsed by pdfplumber."""
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=A4)
    width, height = A4
    y = height - 40
    c.setFont("Helvetica", 10)
    c.drawString(40, y, "Synthetic Bank Statement (text PDF)")
    y -= 20
    c.drawString(40, y, "Buchungstag | Betrag | Waehrung | Empfaenger | IBAN | Verwendungszweck")
    y -= 16

    for _, r in df.head(35).iterrows():
        # Match v3 parser heuristics: dd.mm.yyyy and German amount format 1.234,56
        ymd = str(r["booking_date"])
        yyyy, mm, dd = ymd.split("-")
        d_german = f"{dd}.{mm}.{yyyy}"
        amt = float(r["amount"])
        amt_abs = abs(amt)
        amt_str = f"{amt_abs:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if amt < 0:
            amt_str = "-" + amt_str
        line = f"{d_german}  {amt_str}  {r['currency']}  {r['recipient_name']}  {r['recipient_account']}  {r['transaction_description']}"
        c.drawString(40, y, line[:120])
        y -= 14
        if y < 60:
            c.showPage()
            c.setFont("Helvetica", 10)
            y = height - 40

    c.save()


def seed_demo_data(
    db: Session,
    *,
    case_id: str = "case_0001",
    company_name: str = "Demo Insolvent GmbH",
    account_iban: str = "DE89370400440532013000",
    currency: str = "EUR",
    days_back: int = 60,
) -> SeedResult:
    """Create a demo case with synthetic documents and run the full pipeline.

    This is used to validate the end-to-end pipeline on a fresh installation.
    """

    case = get_case(db, case_id)
    if not case:
        case = create_case(
            db,
            case_id=case_id,
            company_name=company_name,
            accounts=[{"account_number": account_iban, "currency": currency}],
            metadata_json={"seed": True},
        )
        # Commit the parent row to guarantee FK stability even if downstream steps use SAVEPOINTs/rollbacks.
        db.commit()

    ensure_case_dirs(case_id)
    bs_dir = case_dir(case_id) / "source_info" / "bank_statements"

    start = date.today() - timedelta(days=days_back)
    df = _make_statement_df(account_iban=account_iban, start=start)

    csv_path = bs_dir / "demo_statement.csv"
    xlsx_path = bs_dir / "demo_statement.xlsx"
    pdf_path = bs_dir / "demo_statement.pdf"

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    _write_pdf_text(pdf_path, df)

    doc_ids: list[int] = []
    inserted_total = 0
    evaluated_total = 0
    for p, dtype in [(csv_path, "bank_statement"), (xlsx_path, "bank_statement"), (pdf_path, "bank_statement")]:
        doc = create_document(db, case_id, dtype, p.name, str(p), detected_format=p.suffix.lstrip("."))
        db.commit()  # persist document before processing
        res = process_document(db, case_id=case_id, document_id=doc.id)
        inserted_total += int(res.get("inserted", 0))
        evaluated_total += int(res.get("evaluated", 0))
        doc_ids.append(doc.id)

    db.commit()
    return SeedResult(case_id=case_id, documents=doc_ids, inserted=inserted_total, evaluated=evaluated_total)
