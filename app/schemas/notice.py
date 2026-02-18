from __future__ import annotations

from pydantic import BaseModel


class NoticeOut(BaseModel):
    id: int
    case_id: str
    counterparty_name: str
    document_name: str
    file_path: str
    status: str
    content: str
    transaction_ids: list[int] = []

    class Config:
        from_attributes = True


class NoticeUpdate(BaseModel):
    content: str


class NoticeStatusUpdate(BaseModel):
    status: str  # Generated|Accepted|Sent
