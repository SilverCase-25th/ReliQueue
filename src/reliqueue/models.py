from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ValidationError


class JobStatus(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    COMPLETED = "completed"
    DEAD = "dead"
    CANCELLED = "cancelled"


class EmailPayload(BaseModel):
    to: str
    subject: str
    body: str


class ReportPayload(BaseModel):
    report_name: str
    account_id: str


PAYLOAD_SCHEMAS: dict[str, type[BaseModel]] = {
    "email.send": EmailPayload,
    "report.generate": ReportPayload,
}


class EnqueueRequest(BaseModel):
    job_type: str
    payload: dict[str, Any]
    idempotency_key: str | None = None
    run_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    max_retries: int = Field(default=3, ge=0, le=25)
    timeout_seconds: int = Field(default=30, ge=1, le=3600)


class EnqueueResult(BaseModel):
    job_id: UUID
    status: JobStatus
    deduplicated: bool = False


class LeasedJob(BaseModel):
    id: UUID
    job_type: str
    payload: dict[str, Any]
    attempt: int
    max_retries: int
    timeout_seconds: int
    correlation_id: UUID = Field(default_factory=uuid4)
    created_at: datetime


def validate_payload(job_type: str, payload: dict[str, Any]) -> None:
    schema = PAYLOAD_SCHEMAS.get(job_type)
    if schema is None:
        raise ValueError(f"Unsupported job_type: {job_type}")
    try:
        schema.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid payload for {job_type}: {exc}") from exc
