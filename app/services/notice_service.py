from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.core.paths import case_dir


def _safe(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_\-]+", "", s)
    return s[:80] or "counterparty"


def default_notice_text(company_name: str, counterparty_name: str, tx_lines: list[str]) -> str:
    body = "\n".join(["- " + ln for ln in tx_lines])
    return (
        f"To: {counterparty_name}\n"
        f"From: {company_name}\n\n"
        "Subject: Notice regarding transactions\n\n"
        "We refer to the following transactions and request clarification / repayment according to applicable rules.\n\n"
        f"Transactions:\n{body}\n\n"
        "Please respond within 7 days.\n"
    )


def render_notice_pdf(case_id: str, pdf_path: Path, content: str) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    width, height = A4

    x = 40
    y = height - 50
    line_height = 14

    for line in content.splitlines():
        if y < 60:
            c.showPage()
            y = height - 50
        c.drawString(x, y, line[:120])
        y -= line_height

    c.save()


def make_notice_filename(counterparty_name: str, doc_type: str = "notice") -> str:
    ymd = date.today().strftime("%Y%m%d")
    return f"{ymd}_{_safe(counterparty_name)}_{doc_type}.pdf"


def notice_path(case_id: str, filename: str) -> Path:
    return case_dir(case_id) / "notices" / filename
