from __future__ import annotations

from fastapi import APIRouter, Cookie, Depends, Request, Form, UploadFile, File
from typing import Optional
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.paths import ensure_case_dirs, case_dir

from app.api.deps import get_db
from app.repositories.case_repo import list_cases, create_case, update_case, get_case
from app.repositories.document_repo import list_documents, create_document
from app.services.pipeline_service import process_document, run_ocr_and_process
from app.tasks.background import submit as submit_task
from app.services.dashboard_service import (
    get_overview_metrics,
    get_overview_timeseries,
    get_overview_rule_counts,
    get_top_counterparties,
    get_notice_status_counts,
    get_statement_coverage,
    get_high_risk_transactions,
)
from app.services.ui_transactions_service import (
    get_transactions_page,
    get_transaction_detail,
)
from app.services.ui_rules_service import get_rules_review_page
from app.services.ui_dedup_service import get_dedup_clusters_page
from app.services.ui_audit_service import get_audit_page
from app.services.ui_notices_service import get_notices_page, get_notice_detail
from app.services.ui_counterparty_service import get_counterparties_page
from app.tools.seed_demo_data import seed_demo_data

import urllib.parse
import re


templates = Jinja2Templates(directory="app/templates")
templates.env.filters["urlencode"] = lambda s: urllib.parse.quote(str(s), safe="")

ui_router = APIRouter(tags=["ui"], include_in_schema=False)


def _pick_case_id(db, case_id_cookie: Optional[str]) -> Optional[str]:
    cases = list_cases(db)
    if not cases:
        return None
    ids = [c.case_id for c in cases]
    if case_id_cookie and case_id_cookie in ids:
        return case_id_cookie
    # default to most recently created (repo returns by created_at asc/desc? be safe: last)
    return ids[-1]


@ui_router.get("/", response_class=HTMLResponse)
def overview(request: Request, db=Depends(get_db), case_id: Optional[str] = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(
            request,
            "empty.html",
            {"cases": [], "selected_case": None},
            status_code=200,
        )

    metrics = get_overview_metrics(db, selected_case_id)
    ts = get_overview_timeseries(db, selected_case_id)
    rule_counts = get_overview_rule_counts(db, selected_case_id)
    top_cp = get_top_counterparties(db, selected_case_id)
    notice_counts = get_notice_status_counts(db, selected_case_id)
    coverage = get_statement_coverage(db, selected_case_id)
    high_risk = get_high_risk_transactions(db, selected_case_id)

    resp = templates.TemplateResponse(
        request,
        "overview.html",
        {
            "cases": cases,
            "selected_case_id": selected_case_id,
            "metrics": metrics,
            "timeseries": ts,
            "rule_counts": rule_counts,
            "top_counterparties": top_cp,
            "notice_counts": notice_counts,
            "coverage": coverage,
            "high_risk": high_risk,
        },
    )
    # persist selection
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/ui/set_case/{case_id}")
def set_case(case_id: str):
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie("case_id", case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/cases", response_class=HTMLResponse)
def cases_page(request: Request, db=Depends(get_db), case_id: Optional[str] = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    resp = templates.TemplateResponse(
        request,
        "cases.html",
        {"cases": cases, "selected_case_id": selected_case_id},
    )
    if selected_case_id:
        resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/cases/new", response_class=HTMLResponse)
def case_new_form(request: Request, db=Depends(get_db), case_id: Optional[str] = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    return templates.TemplateResponse(
        request,
        "case_form.html",
        {"cases": cases, "selected_case_id": selected_case_id, "mode": "new", "case": None},
    )


def _parse_accounts(raw: str) -> list[dict]:
    """Parse accounts from textarea. Format: one per line: IBAN[,CURRENCY]."""
    out: list[dict] = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",") if p.strip()]
        if not parts:
            continue
        out.append({"account_number": parts[0], "currency": parts[1] if len(parts) > 1 else None})
    return out


@ui_router.post("/cases/new")
def case_create(
    db: Session = Depends(get_db),
    case_id: str = Form(...),
    company_name: str = Form(...),
    accounts_raw: str = Form(default=""),
):
    accounts = _parse_accounts(accounts_raw)
    ensure_case_dirs(case_id.strip())
    create_case(db, case_id=case_id.strip(), company_name=company_name.strip(), accounts=accounts)
    db.commit()
    resp = RedirectResponse(url="/cases", status_code=302)
    resp.set_cookie("case_id", case_id.strip(), max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/cases/{case_id}/edit", response_class=HTMLResponse)
def case_edit_form(request: Request, case_id: str, db=Depends(get_db), case_id_cookie: Optional[str] = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id_cookie)
    c = get_case(db, case_id)
    return templates.TemplateResponse(
        request,
        "case_form.html",
        {"cases": cases, "selected_case_id": selected_case_id, "mode": "edit", "case": c},
    )


@ui_router.post("/cases/{case_id}/edit")
def case_update(
    case_id: str,
    db: Session = Depends(get_db),
    company_name: str = Form(...),
    accounts_raw: str = Form(default=""),
):
    accounts = _parse_accounts(accounts_raw)
    ensure_case_dirs(case_id)
    update_case(db, case_id=case_id, company_name=company_name.strip(), accounts=accounts)
    db.commit()
    resp = RedirectResponse(url="/cases", status_code=302)
    resp.set_cookie("case_id", case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/documents", response_class=HTMLResponse)
def documents_page(request: Request, db=Depends(get_db), case_id: Optional[str] = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    docs = list_documents(db, selected_case_id)
    resp = templates.TemplateResponse(
        request,
        "documents.html",
        {"cases": cases, "selected_case_id": selected_case_id, "documents": docs},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


def _doc_target_dir(case_id: str, document_type: str):
    base = case_dir(case_id) / "source_info"
    if document_type == "bank_statement":
        return base / "bank_statements"
    if document_type == "creditor_list":
        return base / "list_of_creditors"
    d = base / "other"
    d.mkdir(parents=True, exist_ok=True)
    return d


@ui_router.post("/documents/upload")
async def documents_upload(
    db: Session = Depends(get_db),
    case_id: Optional[str] = Cookie(default=None),
    document_type: str = Form(...),
    file: UploadFile = File(...),
):
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return RedirectResponse(url="/cases/new", status_code=302)

    ensure_case_dirs(selected_case_id)
    target_dir = _doc_target_dir(selected_case_id, document_type)
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = target_dir / file.filename
    content = await file.read()
    dst.write_bytes(content)

    doc = create_document(
        db,
        case_id=selected_case_id,
        document_type=document_type,
        file_name=file.filename,
        file_path=str(dst),
    )
    db.commit()

    if document_type == "bank_statement":
        # NOTE: submit_task() takes case_id as the FIRST positional argument.
        # Passing case_id again as a keyword would raise:
        #   TypeError: submit() got multiple values for argument 'case_id'
        submit_task(selected_case_id, process_document, document_id=doc.id)

    return RedirectResponse(url="/documents", status_code=302)


@ui_router.post("/documents/{document_id}/run_ocr")
def documents_run_ocr(document_id: int, db=Depends(get_db), case_id: Optional[str] = Cookie(default=None)):
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return RedirectResponse(url="/cases", status_code=302)
    # Run OCR in background (requires Poppler + Tesseract installed on the host)
    submit_task(selected_case_id, run_ocr_and_process, document_id=document_id)
    return RedirectResponse(url="/documents", status_code=302)


@ui_router.get("/ui/seed_demo")
def seed_demo(db=Depends(get_db)):
    """Create a demo case + documents and run the full pipeline.

    This is meant for a clean install smoke-test and for UI demos.
    """
    res = seed_demo_data(db)
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie("case_id", res.case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/transactions", response_class=HTMLResponse)
def transactions(request: Request, db=Depends(get_db), case_id: Optional[str] = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    page = get_transactions_page(db, selected_case_id, request.query_params)
    resp = templates.TemplateResponse(
        request,
        "transactions.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/transactions/table", response_class=HTMLResponse)
def transactions_table(request: Request, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "partials/transactions_table.html", {"rows": [], "total": 0, "page": 1, "page_size": 50})
    page = get_transactions_page(db, selected_case_id, request.query_params)
    return templates.TemplateResponse(request, "partials/transactions_table.html", page)


@ui_router.get("/transactions/{tx_id}", response_class=HTMLResponse)
def transaction_detail(request: Request, tx_id: int, db=Depends(get_db)):
    detail = get_transaction_detail(db, tx_id)
    return templates.TemplateResponse(request, "partials/tx_detail.html", detail)


@ui_router.get("/rules", response_class=HTMLResponse)
def rules(request: Request, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    page = get_rules_review_page(db, selected_case_id, request.query_params)
    resp = templates.TemplateResponse(
        request,
        "rules.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/counterparties", response_class=HTMLResponse)
def counterparties(request: Request, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    page = get_counterparties_page(db, selected_case_id, request.query_params)
    resp = templates.TemplateResponse(
        request,
        "counterparties.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/dedup", response_class=HTMLResponse)
def dedup(request: Request, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    page = get_dedup_clusters_page(db, selected_case_id, request.query_params)
    resp = templates.TemplateResponse(
        request,
        "dedup.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/audit", response_class=HTMLResponse)
def audit(request: Request, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    page = get_audit_page(db, selected_case_id, request.query_params)
    resp = templates.TemplateResponse(
        request,
        "audit.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/notices", response_class=HTMLResponse)
def notices(request: Request, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})
    page = get_notices_page(db, selected_case_id, request.query_params)
    resp = templates.TemplateResponse(
        request,
        "notices.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.get("/notices/{notice_id}", response_class=HTMLResponse)
def notice_detail(request: Request, notice_id: int, db=Depends(get_db), case_id: str | None = Cookie(default=None)):
    cases = list_cases(db)
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return templates.TemplateResponse(request, "empty.html", {"cases": [], "selected_case": None})

    page = get_notice_detail(db, notice_id)
    if page.get("notice") is None:
        return templates.TemplateResponse(
            request,
            "not_found.html",
            {"cases": cases, "selected_case_id": selected_case_id, "message": "Notice not found"},
            status_code=404,
        )

    resp = templates.TemplateResponse(
        request,
        "notice_detail.html",
        {"cases": cases, "selected_case_id": selected_case_id, **page},
    )
    resp.set_cookie("case_id", selected_case_id, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@ui_router.post("/notices/{notice_id}/status")
def notice_update_status(
    notice_id: int,
    status: str = Form(...),
    db=Depends(get_db),
):
    from app.repositories.notice_repo import update_notice_status

    update_notice_status(db, notice_id, status)
    db.commit()
    return RedirectResponse(url=f"/notices/{notice_id}", status_code=303)


@ui_router.post("/notices/generate_selected")
def notices_generate_selected(
    request: Request,
    db=Depends(get_db),
    case_id: str | None = Cookie(default=None),
    tx_ids: str = Form(default=""),
):
    """Generate notices from comma/space separated tx ids."""
    selected_case_id = _pick_case_id(db, case_id)
    if not selected_case_id:
        return RedirectResponse(url="/", status_code=303)

    # parse ids
    ids: list[int] = []
    for part in re.split(r"[\s,]+", tx_ids.strip()):
        if not part:
            continue
        try:
            ids.append(int(part))
        except Exception:
            continue
    ids = sorted(set(ids))

    if ids:
        from app.api.routers.notices import api_generate_notices, GenerateNoticesIn

        api_generate_notices(GenerateNoticesIn(case_id=selected_case_id, transaction_ids=ids), db=db)
        db.commit()

    return RedirectResponse(url="/notices", status_code=303)
