from __future__ import annotations

from typing import List, Optional

from sqlalchemy.orm import Session

from app.db.models import Document
from app.repositories.audit_repo import log_event


def create_document(
    db: Session,
    case_id: str,
    document_type: str,
    file_name: str,
    file_path: str,
    detected_format: Optional[str] = None,
) -> Document:
    d = Document(
        case_id=case_id,
        document_type=document_type,
        file_name=file_name,
        file_path=file_path,
        detected_format=detected_format,
    )
    db.add(d)
    db.flush()
    log_event(
        db,
        case_id=case_id,
        action="document.uploaded",
        entity_type="document",
        entity_id=str(d.id),
        payload={"document_type": document_type, "file_name": file_name, "file_path": file_path},
    )
    db.flush()
    db.refresh(d)
    return d


def list_documents(db: Session, case_id: str) -> List[Document]:
    return db.query(Document).filter(Document.case_id == case_id).order_by(Document.uploaded_at.desc()).all()


def get_document(db: Session, document_id: int) -> Optional[Document]:
    return db.query(Document).filter(Document.id == document_id).first()
