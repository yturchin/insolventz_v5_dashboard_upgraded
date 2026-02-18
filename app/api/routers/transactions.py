from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.repositories.transaction_repo import list_transactions, update_transaction_tags, to_out
from app.schemas.transaction import TransactionOut, TransactionTagUpdate

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.get("", response_model=List[TransactionOut])
def api_list_transactions(
    case_id: str,
    recipient_name: Optional[str] = None,
    transaction_description: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    tags: Optional[str] = None,  # comma-separated
    order_by: str = "transaction_date",
    order_dir: str = "desc",
    db: Session = Depends(get_db),
):
    tags_any = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
    txs = list_transactions(
        db,
        case_id=case_id,
        recipient_name=recipient_name,
        transaction_description=transaction_description,
        date_from=date_from,
        date_to=date_to,
        tags_any=tags_any,
        order_by=order_by,
        order_dir=order_dir,
    )
    return [to_out(t) for t in txs]


@router.patch("/{tx_id}/tags", response_model=TransactionOut)
def api_update_tags(tx_id: int, payload: TransactionTagUpdate, db: Session = Depends(get_db)):
    try:
        tx = update_transaction_tags(db, tx_id, payload.tags)
        return to_out(tx)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
