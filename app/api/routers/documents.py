from __future__ import annotations

import shutil
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.paths import ensure_case_dirs, case_dir
from app.repositories.case_repo import get_case
from app.repositories.document_repo import create_document, list_documents
from app.services.ingest_service import detect_format
from app.services.pipeline_service import process_document
from app.tasks.background import submit
from app.schemas.document import DocumentOut

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("", response_model=list[DocumentOut])
def api_list_documents(case_id: str, db: Session = Depends(get_db)):
    return list_documents(db, case_id)


@router.post("/upload", response_model=DocumentOut)
def api_upload_document(
    background_tasks: BackgroundTasks,
    case_id: str = Form(...),
    document_type: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    c = get_case(db, case_id)
    if not c:
        raise HTTPException(status_code=404, detail="Case not found")

    ensure_case_dirs(case_id)

    # route by doc type
    dest_dir = case_dir(case_id) / "source_info"
    if document_type.lower() in {"bank_statement", "transaction", "payments", "bank_statements"}:
        dest_dir = dest_dir / "bank_statements"
    elif document_type.lower() in {"list_of_creditors", "creditors"}:
        dest_dir = dest_dir / "list_of_creditors"
    else:
        dest_dir = dest_dir / "other"

    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_path = dest_dir / file.filename
    with dest_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    fmt = detect_format(dest_path).get("doc_type")
    doc = create_document(db, case_id, document_type, file.filename, str(dest_path), detected_format=fmt)

    # Bank statements are processed asynchronously (background thread pool)
    if document_type.lower() in {"bank_statement", "transaction", "payments", "bank_statements"}:
        submit(case_id, process_document, case_id=case_id, document_id=doc.id)

    return doc
