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
"""DTOs (Pydantic request/response models) — re-exported for convenience."""

from ingestor_orchestrator.dto.ckan_instance_response import CkanInstanceResponse
from ingestor_orchestrator.dto.instance_create import InstanceCreate
from ingestor_orchestrator.dto.instance_stats import InstanceStats
from ingestor_orchestrator.dto.job_create import JobCreate
from ingestor_orchestrator.dto.job_list_response import JobListResponse
from ingestor_orchestrator.dto.job_response import JobResponse
from ingestor_orchestrator.dto.job_result_response import JobResultResponse
from ingestor_orchestrator.dto.metadata_sync_response import MetadataSyncResponse
from ingestor_orchestrator.dto.resource_detail_response import ResourceDetailResponse
from ingestor_orchestrator.dto.resource_response import ResourceResponse

__all__ = [
    "CkanInstanceResponse",
    "InstanceCreate",
    "InstanceStats",
    "JobCreate",
    "JobListResponse",
    "JobResponse",
    "JobResultResponse",
    "MetadataSyncResponse",
    "ResourceDetailResponse",
    "ResourceResponse",
]
