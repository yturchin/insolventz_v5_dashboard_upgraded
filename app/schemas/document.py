from __future__ import annotations

from typing import Optional
from pydantic import BaseModel


class DocumentOut(BaseModel):
    id: int
    case_id: str
    document_type: str
    file_name: str
    file_path: str
    detected_format: Optional[str] = None

    class Config:
        from_attributes = True
