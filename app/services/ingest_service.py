from __future__ import annotations

import csv
import hashlib
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Iterable

import pandas as pd


class OCRRequiredError(RuntimeError):
    """Raised when a PDF appears to be image-only and requires OCR."""


# ---------- format detection ----------

def detect_format(file_path: Path) -> dict:
    ext = file_path.suffix.lower()
    info = {"ext": ext, "doc_type": "unknown", "has_text_layer": None}

    if ext == ".csv":
        info["doc_type"] = "bank_statement_csv"
        return info
    if ext in {".xlsx", ".xls"}:
        info["doc_type"] = "bank_statement_xlsx"
        return info
    if ext == ".pdf":
        info["doc_type"] = "bank_statement_pdf_scan"
        try:
            import pdfplumber
            with pdfplumber.open(str(file_path)) as pdf:
                text = "".join((p.extract_text() or "") for p in pdf.pages[:3])
                if len(text.strip()) > 50:
                    info["doc_type"] = "bank_statement_pdf_text"
                    info["has_text_layer"] = True
                else:
                    info["has_text_layer"] = False
        except Exception:
            info["has_text_layer"] = False
        return info

    return info


# ---------- helpers (ported from v3) ----------

def parse_german_amount(raw: str) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip().replace("\n", "").replace(" ", "")
    s = re.sub(r"[A-Z]{3}$", "", s).strip()
    if not s:
        return None
    neg = s.startswith("-")
    s = s.lstrip("-+")
    s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None


def parse_german_date(raw: str) -> Optional[date]:
    if raw is None:
        return None
    s = str(raw).strip().replace("\n", "").strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def normalize_iban(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().replace("\n", "").replace(" ", "").upper()
    if not s or s in {"-", "—"}:
        return None
    if re.match(r"^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$", s):
        return s
    return None


def clean_text(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^TESTDATEN\s*[-–—]\s*", "", s)
    return s


def compute_tx_hash(*, booking_date: date, amount: float, currency: str, debtor_iban: Optional[str], creditor_iban: Optional[str], creditor_name: Optional[str], purpose: Optional[str], end_to_end_id: Optional[str]) -> str:
    """
    Stable exact-match hash aligned with v3 dedup key.
    Matches on: booking_date, amount(2dp), currency, debtor IBAN, creditor IBAN, creditor name, purpose, end_to_end_id.
    """
    key = "|".join([
        booking_date.isoformat() if booking_date else "",
        f"{float(amount or 0.0):.2f}",
        (currency or "EUR").upper(),
        (debtor_iban or "").strip().upper(),
        (creditor_iban or "").strip().upper(),
        (creditor_name or "").strip().lower(),
        (purpose or "").strip().lower(),
        (end_to_end_id or "").strip().upper(),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def load_dataframe(file_path: Path) -> pd.DataFrame:
    ext = file_path.suffix.lower()
    if ext == ".csv":
        # delimiter sniff
        with file_path.open("r", encoding="utf-8-sig") as f:
            sample = f.read(4096)
        delim = ";" if sample.count(";") >= sample.count(",") else ","
        return pd.read_csv(file_path, sep=delim)

    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)

    if ext == ".pdf":
        # try text extraction → fallback OCR
        info = detect_format(file_path)
        if info.get("has_text_layer") is True:
            return _pdf_text_to_df(file_path)
        # OCR is handled separately (optional external deps). Signal caller.
        raise OCRRequiredError("PDF appears to be image-only (no text layer). OCR required.")

    raise ValueError(f"Unsupported file type: {ext}")


def _pdf_text_to_df(file_path: Path) -> pd.DataFrame:
    import pdfplumber
    with pdfplumber.open(str(file_path)) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    return pdf_text_to_df_from_text(text)


def pdf_text_to_df_from_text(text: str) -> pd.DataFrame:
    """Parse extracted PDF text (from pdfplumber or OCR) into a dataframe."""
    rows: list[dict] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.search(r"(\d{2}\.\d{2}\.\d{4}).*?([-+]?\d{1,3}(?:\.\d{3})*,\d{2})", line)
        if not m:
            continue
        d = parse_german_date(m.group(1))
        a = parse_german_amount(m.group(2))
        if d and a is not None:
            rows.append({"Date": d.isoformat(), "Amount": a, "Description": line})
    if not rows:
        raise ValueError("PDF text parsed but no transactions recognized")
    return pd.DataFrame(rows)


# ---------- mapping dataframe → transactions ----------

def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols:
            return cols[key]
    # fuzzy contains
    for cand in candidates:
        key = cand.lower()
        for c in df.columns:
            if key in str(c).lower():
                return c
    return None


def dataframe_to_transactions(
    df: pd.DataFrame,
    *,
    case_id: str,
    source_file: str,
    default_source_account: Optional[str] = None,
    default_currency: Optional[str] = None,
) -> list[dict]:
    # Common columns for German statements
    col_date = _pick_col(df, ["Buchungstag", "Buchungsdatum", "Date", "Datum", "transaction_date"])
    col_amount = _pick_col(df, ["Betrag", "Umsatz", "Amount", "amount"])
    col_curr = _pick_col(df, ["Währung", "Waehrung", "Currency", "currency"])
    col_desc = _pick_col(df, ["Verwendungszweck", "Buchungsdetails", "Description", "Zweck", "transaction_description"])
    col_cp = _pick_col(df, ["Empfänger", "Empfänger/Zahlungspflichtiger", "Auftraggeber/Empfänger", "counterparty", "recipient_name", "Name"])
    col_iban = _pick_col(df, ["IBAN Gegenkonto", "Kontonummer/IBAN", "IBAN", "recipient_account"])

    # v3 parity extras (if present in exports)
    col_value_date = _pick_col(df, ["Valutadatum", "Wertstellung", "value_date", "Value Date", "Valuta"])
    col_debtor_iban = _pick_col(df, ["Auftraggeber IBAN", "Debtor IBAN", "Zahlungspflichtiger IBAN", "DebtorAccount"])
    col_creditor_iban = _pick_col(df, ["Empfänger IBAN", "Creditor IBAN", "Beguenstigter IBAN", "recipient_account", "IBAN Gegenkonto"])
    col_debtor_name = _pick_col(df, ["Auftraggeber", "Zahlungspflichtiger", "Debtor Name", "Debtor"])
    col_creditor_name = _pick_col(df, ["Empfänger", "Beguenstigter", "Creditor Name", "Creditor", "Name"])
    col_e2e = _pick_col(df, ["End-to-End-Referenz", "EndToEnd", "End-to-End", "EndToEndId", "E2E"])
    col_bank_ref = _pick_col(df, ["Kundenreferenz", "Bankreferenz", "Mandatsreferenz", "Reference", "Bank Reference"])

    if not col_date or not col_amount:
        raise ValueError(f"Cannot map statement columns. Have: {list(df.columns)}")

    txs: list[dict] = []
    for _, r in df.iterrows():
        d_raw = r.get(col_date)
        d = parse_german_date(str(d_raw)) if not isinstance(d_raw, date) else d_raw
        if not d:
            # try ISO
            try:
                d = datetime.fromisoformat(str(d_raw)).date()
            except Exception:
                continue

        amount = parse_german_amount(r.get(col_amount))
        if amount is None:
            try:
                amount = float(r.get(col_amount))
            except Exception:
                continue

        # value date
        vd = None
        if col_value_date:
            vd_raw = r.get(col_value_date)
            vd = parse_german_date(str(vd_raw)) if not isinstance(vd_raw, date) else vd_raw

        recipient_name = clean_text(r.get(col_cp)) if col_cp else ""
        description = clean_text(r.get(col_desc)) if col_desc else ""

        recipient_account = normalize_iban(r.get(col_iban)) if col_iban else None
        debtor_iban = normalize_iban(r.get(col_debtor_iban)) if col_debtor_iban else (normalize_iban(default_source_account) if default_source_account else None)
        creditor_iban = normalize_iban(r.get(col_creditor_iban)) if col_creditor_iban else recipient_account

        debtor_name = clean_text(r.get(col_debtor_name)) if col_debtor_name else None
        creditor_name = clean_text(r.get(col_creditor_name)) if col_creditor_name else (recipient_name or None)

        e2e = clean_text(r.get(col_e2e)) if col_e2e else None
        bank_ref = clean_text(r.get(col_bank_ref)) if col_bank_ref else None

        currency = (str(r.get(col_curr)).strip() if col_curr else None) or default_currency

        tx_hash = compute_tx_hash(
            booking_date=d,
            amount=amount,
            currency=currency,
            debtor_iban=debtor_iban,
            creditor_iban=creditor_iban,
            creditor_name=creditor_name or recipient_name,
            purpose=description,
            end_to_end_id=e2e,
        )

        txs.append(
            {
                "case_id": case_id,
                "source_document_id": None,
                "source_file": source_file,
                "import_batch_id": None,
                "booking_date": d,
                "value_date": vd,
                "amount": float(amount),
                "currency": currency,
                "debtor_account_iban": debtor_iban,
                "creditor_account_iban": creditor_iban,
                "creditor_name": creditor_name or None,
                "debtor_name": debtor_name,
                "purpose": description or None,
                "end_to_end_id": e2e or None,
                "bank_reference": bank_ref or None,
                # v3-parity raw fields
                "counterparty_name_raw": (creditor_name or recipient_name) or None,
                "raw_description": description or None,
                "normalized_description": description or None,
                "booking_text": None,
                "bic": None,
                # legacy / UI columns
                "source_account": default_source_account,
                "transaction_date": d.isoformat(),
                "recipient_account": recipient_account,
                "recipient_name": (creditor_name or recipient_name) or None,
                "transaction_description": description or None,
                "verified_recipient_id": None,
                "system_tags": [],
                "user_tags": [],
                "user_tags_confirmed": False,
                "tags": "[]",
                "tx_hash": tx_hash,
                "counterparty_id": None,
            }
        )

    return txs