from typing import Literal

from pydantic import BaseModel


class ScanResult(BaseModel):
    id: str
    status: Literal["ERROR", "PENDING", "NO_FOUND", "CLEAN", "INFECTED"]
    key: str | None = None
    bucket: str | None = None
    virus: str | None = None
    details: str | None = None
    original_filename: str | None = None
    timestamp: str | None = None
