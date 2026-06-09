# Pedalin
# Copyright (C) 2025  Pedalin

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from datetime import datetime

from pydantic import BaseModel, computed_field

from ingestor_orchestrator.config import settings
from ingestor_orchestrator.models import JobStatus


class JobCreate(BaseModel):
    resource_id: str
    dataset_name: str
    resource_name: str | None = None
    resource_url: str | None = None
    resource_format: str | None = None


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
    dataset_name: str
    status: JobStatus
    idempotency_key: str
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    model_config = {"from_attributes": True}

    @computed_field
    @property
    def ckan_resource_url(self) -> str:
        """Link to the resource page on the CKAN portal."""
        base = settings.ckan_url.rstrip("/")
        return f"{base}/dataset/{self.dataset_name}/resource/{self.resource_id}"


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
