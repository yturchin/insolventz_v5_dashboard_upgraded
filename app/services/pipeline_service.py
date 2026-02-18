from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.db.models import Case, Document, Transaction, Counterparty, RuleEvaluation
from app.repositories.audit_repo import log_event
from app.repositories.counterparty_repo import get_or_create_counterparty
from app.services.ingest_service import detect_format, load_dataframe, dataframe_to_transactions, OCRRequiredError, pdf_text_to_df_from_text
from app.services.ocr_service import ocr_pdf_to_text, OCRDependencyError
from app.services.rules.rule_engine_service import evaluate_all
from app.services.dedup_service import run_dedup


def process_document(db: Session, *, case_id: str, document_id: int) -> dict:
    """v3-parity pipeline:

    1) Parse document → insert transactions (including potential overlaps)
    2) Run cross-source dedup (mark duplicates, keep canonical)
    3) Evaluate InsO rules for non-duplicate transactions and update rule_hits/system_tags
    """

    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise ValueError("Case not found")

    doc = db.query(Document).filter(Document.id == document_id, Document.case_id == case_id).first()
    if not doc:
        raise ValueError("Document not found")

    p = Path(doc.file_path)
    doc.processing_status = "processing"
    doc.processing_error = None
    # progress fields (for OCR flows)
    if hasattr(doc, "ocr_progress"):
        doc.ocr_progress = 0
    db.flush()

    try:
        info = detect_format(p)
        doc.detected_format = info.get("doc_type")

        df = load_dataframe(p)
    except OCRRequiredError as e:
        doc.processing_status = "ocr_required"
        doc.processing_error = str(e)
        db.flush()
        log_event(db, case_id=case_id, action="document.ocr_required", entity_type="document", entity_id=str(doc.id), payload={"file": doc.file_name})
        return {"status": "ocr_required", "detected_format": doc.detected_format}
    except Exception as e:
        doc.processing_status = "failed"
        doc.processing_error = str(e)
        from datetime import datetime
        doc.processed_at = datetime.utcnow()
        log_event(db, case_id=case_id, action="document.process_failed", entity_type="document", entity_id=str(doc.id), payload={"error": str(e), "file": doc.file_name})
        db.flush()
        raise

    # Defaults: first company account
    default_acc = case.accounts[0].account_number if case.accounts else None
    default_cur = case.accounts[0].currency if case.accounts else None

    tx_dicts = dataframe_to_transactions(
        df,
        case_id=case_id,
        source_file=str(p),
        default_source_account=default_acc,
        default_currency=default_cur,
    )

    inserted = 0

    for t in tx_dicts:
        # IMPORTANT: never call session.rollback() in this loop.
        # A rollback would unwind the whole transaction (case/doc creation, etc.) and break FK consistency.
        # Use a SAVEPOINT per row to skip bad rows (e.g. unique constraint on tx_hash) without nuking the session.
        try:
            with db.begin_nested():
                # counterparty linking
                cp = get_or_create_counterparty(
                    db,
                    case_id=case_id,
                    name=t.get("recipient_name"),
                    account_number=t.get("recipient_account"),
                )
                t["counterparty_id"] = cp.id if cp else None

                # Map v4 ingestion dict → richer Transaction fields (v3 parity)
                tx = Transaction(**t)
                db.add(tx)
                db.flush()
                inserted += 1
        except IntegrityError:
            # duplicate tx_hash or other integrity issue → skip
            continue

    # 2) Dedup across all sources within the case
    dedup_stats = run_dedup(db, case_id=case_id)

    # 3) Rule evaluation for canonical (non-duplicate) tx
    # Clear existing evaluations for this case to keep parity predictable
    db.query(RuleEvaluation).filter(RuleEvaluation.case_id == case_id).delete()

    canonical_txs = db.query(Transaction).filter(Transaction.case_id == case_id, Transaction.is_duplicate == False).all()
    evaluated = 0

    for tx in canonical_txs:
        cp = None
        if tx.counterparty_id:
            cp = db.query(Counterparty).filter(Counterparty.id == tx.counterparty_id).first()

        results = evaluate_all(tx, case, cp)

        # v3: build rule_hits + system_tags
        hits = []
        sys_tags = list(tx.system_tags or [])
        # inflow/outflow
        if "INFLOW" not in sys_tags and tx.amount > 0:
            sys_tags.append("INFLOW")
        if "OUTFLOW" not in sys_tags and tx.amount < 0:
            sys_tags.append("OUTFLOW")

        for r in results:
            db.add(
                RuleEvaluation(
                    case_id=case_id,
                    transaction_id=tx.id,
                    rule_id=r.rule_id,
                    rule_version=r.rule_version,
                    decision=r.decision,
                    confidence=r.confidence,
                    explanation=r.explanation,
                    legal_basis=r.legal_basis,
                    lookback_start=r.lookback_start,
                    lookback_end=r.lookback_end,
                    conditions_met=r.conditions_met or [],
                    conditions_missing=r.conditions_missing or [],
                    evidence_present=r.evidence_present or [],
                    evidence_missing=r.evidence_missing or [],
                )
            )

            if r.decision in ("HIT", "NEEDS_REVIEW"):
                hits.append(
                    {
                        "rule_id": r.rule_id,
                        "decision": r.decision,
                        "confidence": r.confidence,
                        "explanation": r.explanation,
                        "missing_evidence": r.evidence_missing or [],
                    }
                )
                tag = f"ANFECHTUNG_{r.rule_id}"
                if tag not in sys_tags:
                    sys_tags.append(tag)
                if r.decision == "HIT" and "CLAWBACK_CANDIDATE" not in sys_tags:
                    sys_tags.append("CLAWBACK_CANDIDATE")
                if r.decision == "NEEDS_REVIEW" and "NEEDS_REVIEW" not in sys_tags:
                    sys_tags.append("NEEDS_REVIEW")

        tx.rule_hits = hits
        tx.system_tags = sys_tags

        # Backlog/UI combined tags remain compatible
        tags = set(json.loads(tx.tags or "[]"))
        for st in sys_tags:
            tags.add(st)
        tx.tags = json.dumps(sorted(tags))

        evaluated += 1

    db.flush()

    from datetime import datetime
    doc.processing_status = "done"
    doc.processed_at = datetime.utcnow()

    log_event(
        db,
        case_id=case_id,
        action="document.processed",
        entity_type="document",
        entity_id=str(doc.id),
        payload={"file": doc.file_name, "inserted": inserted, "dedup": dedup_stats, "evaluated": evaluated, "detected_format": doc.detected_format},
    )

    return {"status": "done", "inserted": inserted, "dedup": dedup_stats, "evaluated": evaluated, "detected_format": doc.detected_format}


def run_ocr_and_process(db: Session, *, case_id: str, document_id: int) -> dict:
    """Run OCR for an image-based PDF and then process as a bank statement."""

    case = db.query(Case).filter(Case.case_id == case_id).first()
    if not case:
        raise ValueError("Case not found")

    doc = db.query(Document).filter(Document.id == document_id, Document.case_id == case_id).first()
    if not doc:
        raise ValueError("Document not found")

    p = Path(doc.file_path)
    doc.processing_status = "ocr_running"
    doc.processing_error = None
    if hasattr(doc, "ocr_progress"):
        doc.ocr_progress = 0
    db.flush()

    def _progress(cur: int, total: int) -> None:
        if hasattr(doc, "ocr_progress"):
            doc.ocr_progress = int(round((cur / max(total, 1)) * 100))
            db.flush()

    try:
        text = ocr_pdf_to_text(p, on_progress=_progress)
        ocr_txt = p.with_suffix(p.suffix + ".ocr.txt")
        ocr_txt.write_text(text, encoding="utf-8")
        if hasattr(doc, "ocr_text_path"):
            doc.ocr_text_path = str(ocr_txt)
        df = pdf_text_to_df_from_text(text)
    except OCRDependencyError as e:
        doc.processing_status = "failed"
        doc.processing_error = str(e)
        db.flush()
        log_event(db, case_id=case_id, action="document.ocr_failed", entity_type="document", entity_id=str(doc.id), payload={"error": str(e)})
        raise
    except Exception as e:
        doc.processing_status = "failed"
        doc.processing_error = str(e)
        db.flush()
        log_event(db, case_id=case_id, action="document.ocr_failed", entity_type="document", entity_id=str(doc.id), payload={"error": str(e)})
        raise

    # From here we perform the same steps as in process_document, but using the OCR dataframe.

    # Defaults: first company account
    default_acc = case.accounts[0].account_number if case.accounts else None
    default_cur = case.accounts[0].currency if case.accounts else None

    tx_dicts = dataframe_to_transactions(
        df,
        case_id=case_id,
        source_file=str(p),
        default_source_account=default_acc,
        default_currency=default_cur,
    )

    inserted = 0
    for t in tx_dicts:
        try:
            with db.begin_nested():
                cp = get_or_create_counterparty(
                    db,
                    case_id=case_id,
                    name=t.get("recipient_name"),
                    account_number=t.get("recipient_account"),
                )
                t["counterparty_id"] = cp.id if cp else None
                tx = Transaction(**t)
                db.add(tx)
                db.flush()
                inserted += 1
        except IntegrityError:
            continue

    dedup_stats = run_dedup(db, case_id=case_id)

    db.query(RuleEvaluation).filter(RuleEvaluation.case_id == case_id).delete()
    canonical_txs = db.query(Transaction).filter(Transaction.case_id == case_id, Transaction.is_duplicate == False).all()
    evaluated = 0

    for tx in canonical_txs:
        cp = None
        if tx.counterparty_id:
            cp = db.query(Counterparty).filter(Counterparty.id == tx.counterparty_id).first()

        results = evaluate_all(tx, case, cp)
        hits = []
        sys_tags = list(tx.system_tags or [])
        if "INFLOW" not in sys_tags and tx.amount > 0:
            sys_tags.append("INFLOW")
        if "OUTFLOW" not in sys_tags and tx.amount < 0:
            sys_tags.append("OUTFLOW")

        for r in results:
            db.add(
                RuleEvaluation(
                    case_id=case_id,
                    transaction_id=tx.id,
                    rule_id=r.rule_id,
                    rule_version=r.rule_version,
                    decision=r.decision,
                    confidence=r.confidence,
                    explanation=r.explanation,
                    legal_basis=r.legal_basis,
                    lookback_start=r.lookback_start,
                    lookback_end=r.lookback_end,
                    conditions_met=r.conditions_met or [],
                    conditions_missing=r.conditions_missing or [],
                    evidence_present=r.evidence_present or [],
                    evidence_missing=r.evidence_missing or [],
                )
            )

            if r.decision in ("HIT", "NEEDS_REVIEW"):
                hits.append(
                    {
                        "rule_id": r.rule_id,
                        "decision": r.decision,
                        "confidence": r.confidence,
                        "explanation": r.explanation,
                        "missing_evidence": r.evidence_missing or [],
                    }
                )
                tag = f"ANFECHTUNG_{r.rule_id}"
                if tag not in sys_tags:
                    sys_tags.append(tag)
                if r.decision == "HIT" and "CLAWBACK_CANDIDATE" not in sys_tags:
                    sys_tags.append("CLAWBACK_CANDIDATE")
                if r.decision == "NEEDS_REVIEW" and "NEEDS_REVIEW" not in sys_tags:
                    sys_tags.append("NEEDS_REVIEW")

        tx.rule_hits = hits
        tx.system_tags = sys_tags
        tags = set(json.loads(tx.tags or "[]"))
        for st in sys_tags:
            tags.add(st)
        tx.tags = json.dumps(sorted(tags))
        evaluated += 1

    from datetime import datetime
    doc.processing_status = "ocr_done"
    doc.processed_at = datetime.utcnow()
    doc.processing_error = None
    if hasattr(doc, "ocr_progress"):
        doc.ocr_progress = 100
    db.flush()

    log_event(
        db,
        case_id=case_id,
        action="document.ocr_done",
        entity_type="document",
        entity_id=str(doc.id),
        payload={"file": doc.file_name, "inserted": inserted, "dedup": dedup_stats, "evaluated": evaluated},
    )

    return {"status": "ocr_done", "inserted": inserted, "dedup": dedup_stats, "evaluated": evaluated, "detected_format": doc.detected_format}
