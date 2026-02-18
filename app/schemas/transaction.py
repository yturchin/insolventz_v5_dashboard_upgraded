from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel


class TransactionOut(BaseModel):
    id: int
    case_id: str
    source_account: Optional[str]
    currency: Optional[str]
    transaction_date: str
    recipient_account: Optional[str]
    recipient_name: Optional[str]
    transaction_description: Optional[str]
    amount: float
    verified_recipient_id: Optional[str]
    tags: List[str]
    system_tags: List[str] = []
    rule_hits: List[dict] = []
    source_file: Optional[str]
    is_duplicate: bool = False
    duplicate_of: Optional[int] = None

    class Config:
        from_attributes = True


class TransactionTagUpdate(BaseModel):
    tags: List[str]
