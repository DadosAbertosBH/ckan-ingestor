from datetime import datetime

from pydantic import BaseModel

from ingestor_orchestrator.models import JobStatus


class JobCreate(BaseModel):
    resource_id: str
    resource_name: str | None = None
    resource_url: str | None = None
    resource_format: str | None = None
    dataset_name: str | None = None


class JobResultResponse(BaseModel):
    id: str
    job_id: str
    success: bool
    error_message: str | None
    error_trace: str | None
    dataset_preview: dict | list | None
    rows_processed: int | None
    created_at: datetime

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    id: str
    resource_id: str
    resource_name: str | None
    resource_url: str | None
    resource_format: str | None
    dataset_name: str | None
    status: JobStatus
    idempotency_key: str
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class JobResponse(JobListResponse):
    results: list[JobResultResponse] = []

    model_config = {"from_attributes": True}


class DashboardStats(BaseModel):
    total_jobs: int
    pending: int
    processing: int
    completed: int
    failed: int
    last_24h: int
