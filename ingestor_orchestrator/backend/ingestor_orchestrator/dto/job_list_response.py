# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

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
    instance_name: str | None = None
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
