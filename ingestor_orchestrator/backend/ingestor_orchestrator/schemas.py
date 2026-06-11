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

from pydantic import BaseModel

from ingestor_orchestrator.models import JobStatus


class CkanInstanceResponse(BaseModel):
    id: str
    name: str
    url: str
    last_metadata_synced: datetime | None = None
    dataset_count: int = 0
    resource_count: int = 0
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class InstanceStats(BaseModel):
    instance: CkanInstanceResponse
    pending: int = 0
    processing: int = 0
    completed: int = 0
    failed: int = 0


class InstanceCreate(BaseModel):
    name: str
    url: str


class JobCreate(BaseModel):
    resource_id: str
    dataset_name: str
    resource_name: str | None = None
    resource_url: str | None = None
    resource_format: str | None = None
    instance_id: str | None = None
    ckan_url: str = ""


class JobResultResponse(BaseModel):
    id: str
    job_id: str
    success: bool
    error_message: str | None
    error_trace: str | None
    dataset_preview: dict | list | None
    rows_processed: int | None
    expected_rows: int | None = None
    resource_size: int | None = None
    encoding: str | None = None
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
    instance_id: str | None = None
    ckan_resource_url: str = ""
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    labels: list[str] = []
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None

    model_config = {"from_attributes": True}


class JobResponse(JobListResponse):
    results: list[JobResultResponse] = []

    model_config = {"from_attributes": True}


class ResourceResponse(BaseModel):
    resource_id: str
    resource_name: str | None
    resource_url: str | None
    resource_format: str | None
    dataset_name: str
    status: JobStatus
    instance_id: str
    ckan_resource_url: str = ""
    labels: list[str] = []
    job_count: int = 0
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ResourceDetailResponse(ResourceResponse):
    latest_job: JobResponse | None = None
    jobs: list[JobListResponse] = []
    preview: list[dict] = []

    model_config = {"from_attributes": True}
