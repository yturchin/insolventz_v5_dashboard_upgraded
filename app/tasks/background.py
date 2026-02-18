from __future__ import annotations

import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable, Any

from app.core.database import SessionLocal
from app.repositories.audit_repo import log_event


_executor = ThreadPoolExecutor(max_workers=2)


def submit(case_id: Optional[str], fn: Callable[..., Any], *args, **kwargs) -> None:
    """Fire-and-forget background execution.

    Uses a small thread pool. Each job gets its own DB session.
    """

    def _wrapped():
        db = SessionLocal()
        try:
            log_event(db, case_id=case_id, action="task.started", entity_type="task", payload={"fn": getattr(fn, "__name__", str(fn))})
            db.commit()
            fn(db, *args, **kwargs)
            db.commit()
            log_event(db, case_id=case_id, action="task.completed", entity_type="task", payload={"fn": getattr(fn, "__name__", str(fn))})
            db.commit()
        except Exception as e:
            db.rollback()
            log_event(
                db,
                case_id=case_id,
                action="task.failed",
                entity_type="task",
                payload={"fn": getattr(fn, "__name__", str(fn)), "error": str(e), "trace": traceback.format_exc()[:4000]},
            )
            db.commit()
        finally:
            db.close()

    _executor.submit(_wrapped)
