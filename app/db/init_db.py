from __future__ import annotations

from sqlalchemy import text

from app.core.database import engine
from .base import Base

# Ensure ORM models are imported so Base.metadata is populated before create_all().
from . import models  # noqa: F401


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    # Enforce FK constraints for sqlite
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))
        # Best-effort lightweight migrations for existing SQLite DBs
        # (SQLite has limited ALTER support; we only add missing columns/tables.)
        try:
            conn.execute(text("CREATE TABLE IF NOT EXISTS dedup_decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, case_id TEXT, transaction_id INTEGER, decision TEXT NOT NULL, duplicate_of INTEGER, method TEXT, confidence REAL, reason TEXT, details JSON, created_at DATETIME)"))
        except Exception:
            pass

        # Best-effort SQLite schema migrations.
        # NOTE: SQLite doesn't support many ALTER operations; we only add missing columns.

        def _cols(table: str) -> set[str]:
            try:
                rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
                # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                return {r[1] for r in rows}
            except Exception:
                return set()

        tx_cols = _cols("transactions")

        # If this installation has an older schema without core columns (e.g. booking_date),
        # we must rebuild the table (SQLite can't ALTER ADD COLUMN for NOT NULL + existing rows
        # in a reliable way). This keeps existing data best-effort.
        if tx_cols and "booking_date" not in tx_cols:
            try:
                conn.execute(text("ALTER TABLE transactions RENAME TO transactions_old"))
                # Minimal DDL matching current ORM model.
                conn.execute(
                    text(
                        """
                        CREATE TABLE transactions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            case_id VARCHAR NOT NULL,
                            source_document_id INTEGER,
                            source_file VARCHAR,
                            import_batch_id VARCHAR,
                            booking_date DATE NOT NULL,
                            value_date DATE,
                            amount FLOAT NOT NULL,
                            currency VARCHAR NOT NULL,
                            debtor_account_iban VARCHAR,
                            creditor_account_iban VARCHAR,
                            creditor_name VARCHAR,
                            debtor_name VARCHAR,
                            purpose TEXT,
                            end_to_end_id VARCHAR,
                            bank_reference VARCHAR,
                            counterparty_id INTEGER,
                            counterparty_name_raw VARCHAR,
                            raw_description TEXT,
                            normalized_description TEXT,
                            booking_text VARCHAR,
                            bic VARCHAR,
                            rule_hits JSON,
                            source_account VARCHAR,
                            transaction_date DATE,
                            recipient_account VARCHAR,
                            recipient_name VARCHAR,
                            transaction_description TEXT,
                            verified_recipient_id VARCHAR,
                            tags JSON,
                            system_tags JSON,
                            user_tags JSON,
                            user_tags_confirmed BOOLEAN DEFAULT 0,
                            tx_hash VARCHAR,
                            is_duplicate BOOLEAN DEFAULT 0,
                            duplicate_of INTEGER,
                            dedup_cluster_id INTEGER,
                            created_at DATETIME,
                            FOREIGN KEY(case_id) REFERENCES cases(case_id),
                            FOREIGN KEY(source_document_id) REFERENCES documents(id),
                            FOREIGN KEY(counterparty_id) REFERENCES counterparties(id)
                        )
                        """
                    )
                )

                old_cols = _cols("transactions_old")
                # Column mapping: keep what exists, otherwise synthesize.
                mapping = {
                    "case_id": "case_id" if "case_id" in old_cols else "NULL",
                    "source_document_id": "source_document_id" if "source_document_id" in old_cols else "NULL",
                    "source_file": "source_file" if "source_file" in old_cols else "NULL",
                    "import_batch_id": "import_batch_id" if "import_batch_id" in old_cols else "NULL",
                    "booking_date": "booking_date" if "booking_date" in old_cols else ("transaction_date" if "transaction_date" in old_cols else "date('now')"),
                    "value_date": "value_date" if "value_date" in old_cols else "NULL",
                    "amount": "amount" if "amount" in old_cols else "0.0",
                    "currency": "currency" if "currency" in old_cols else "'EUR'",
                    "debtor_account_iban": "debtor_account_iban" if "debtor_account_iban" in old_cols else ("source_account" if "source_account" in old_cols else "NULL"),
                    "creditor_account_iban": "creditor_account_iban" if "creditor_account_iban" in old_cols else ("recipient_account" if "recipient_account" in old_cols else "NULL"),
                    "creditor_name": "creditor_name" if "creditor_name" in old_cols else ("recipient_name" if "recipient_name" in old_cols else "NULL"),
                    "debtor_name": "debtor_name" if "debtor_name" in old_cols else "NULL",
                    "purpose": "purpose" if "purpose" in old_cols else ("transaction_description" if "transaction_description" in old_cols else "NULL"),
                    "end_to_end_id": "end_to_end_id" if "end_to_end_id" in old_cols else "NULL",
                    "bank_reference": "bank_reference" if "bank_reference" in old_cols else "NULL",
                    "counterparty_id": "counterparty_id" if "counterparty_id" in old_cols else "NULL",
                    "counterparty_name_raw": "counterparty_name_raw" if "counterparty_name_raw" in old_cols else "NULL",
                    "raw_description": "raw_description" if "raw_description" in old_cols else "NULL",
                    "normalized_description": "normalized_description" if "normalized_description" in old_cols else "NULL",
                    "booking_text": "booking_text" if "booking_text" in old_cols else "NULL",
                    "bic": "bic" if "bic" in old_cols else "NULL",
                    "rule_hits": "rule_hits" if "rule_hits" in old_cols else ("'[]'"),
                    "source_account": "source_account" if "source_account" in old_cols else "NULL",
                    "transaction_date": "transaction_date" if "transaction_date" in old_cols else ("booking_date" if "booking_date" in old_cols else "date('now')"),
                    "recipient_account": "recipient_account" if "recipient_account" in old_cols else "NULL",
                    "recipient_name": "recipient_name" if "recipient_name" in old_cols else "NULL",
                    "transaction_description": "transaction_description" if "transaction_description" in old_cols else "NULL",
                    "verified_recipient_id": "verified_recipient_id" if "verified_recipient_id" in old_cols else "NULL",
                    "tags": "tags" if "tags" in old_cols else "'[]'",
                    "system_tags": "system_tags" if "system_tags" in old_cols else "'[]'",
                    "user_tags": "user_tags" if "user_tags" in old_cols else "'[]'",
                    "user_tags_confirmed": "user_tags_confirmed" if "user_tags_confirmed" in old_cols else "0",
                    "tx_hash": "tx_hash" if "tx_hash" in old_cols else "NULL",
                    "is_duplicate": "is_duplicate" if "is_duplicate" in old_cols else "0",
                    "duplicate_of": "duplicate_of" if "duplicate_of" in old_cols else "NULL",
                    "dedup_cluster_id": "dedup_cluster_id" if "dedup_cluster_id" in old_cols else "NULL",
                    "created_at": "created_at" if "created_at" in old_cols else "CURRENT_TIMESTAMP",
                }

                cols = ",".join(mapping.keys())
                sel = ",".join(mapping[c] for c in mapping.keys())
                conn.execute(text(f"INSERT INTO transactions ({cols}) SELECT {sel} FROM transactions_old"))
                conn.execute(text("DROP TABLE transactions_old"))
                conn.commit()
                tx_cols = _cols("transactions")
            except Exception:
                # If rebuild fails, continue with best-effort add-column path.
                pass
        tx_add: list[str] = []

        # Added for UI joins and document traceability
        if "source_document_id" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN source_document_id INTEGER")
        if "import_batch_id" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN import_batch_id VARCHAR")

        # Added for parity / dedup / forensic
        if "is_duplicate" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN is_duplicate BOOLEAN DEFAULT 0")
        if "duplicate_of" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN duplicate_of INTEGER")
        if "dedup_cluster_id" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN dedup_cluster_id INTEGER")
        if "rule_hits" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN rule_hits JSON")
        if "counterparty_name_raw" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN counterparty_name_raw VARCHAR")
        if "raw_description" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN raw_description TEXT")
        if "normalized_description" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN normalized_description TEXT")
        if "booking_text" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN booking_text VARCHAR")
        if "bic" not in tx_cols:
            tx_add.append("ALTER TABLE transactions ADD COLUMN bic VARCHAR")

        for ddl in tx_add:
            try:
                conn.execute(text(ddl))
            except Exception:
                pass

        # Add columns to documents if they don't exist
        for ddl in [
            "ALTER TABLE documents ADD COLUMN processing_status VARCHAR",
            "ALTER TABLE documents ADD COLUMN processing_error TEXT",
            "ALTER TABLE documents ADD COLUMN processed_at DATETIME",
            "ALTER TABLE documents ADD COLUMN ocr_progress INTEGER DEFAULT 0",
            "ALTER TABLE documents ADD COLUMN ocr_text_path VARCHAR",
        ]:
            try:
                conn.execute(text(ddl))
            except Exception:
                pass
        conn.commit()
