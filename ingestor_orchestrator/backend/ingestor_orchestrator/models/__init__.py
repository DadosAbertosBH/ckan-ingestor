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
"""CKAN Orchestrator database models — re-exported for backward compatibility."""

from ingestor_orchestrator.models.base import new_uuid, utcnow
from ingestor_orchestrator.models.ckan_data_job import CkanDataJob
from ingestor_orchestrator.models.ckan_data_job_result import CkanDataJobResult
from ingestor_orchestrator.models.ckan_instance import CkanInstance
from ingestor_orchestrator.models.job_status import JobStatus
from ingestor_orchestrator.models.latest_resource_job import LatestResourceJob
from ingestor_orchestrator.models.metadata_sync import MetadataSync
from ingestor_orchestrator.models.resource_metadata_label import ResourceMetadataLabel

__all__ = [
    "CkanDataJob",
    "CkanDataJobResult",
    "CkanInstance",
    "JobStatus",
    "LatestResourceJob",
    "MetadataSync",
    "ResourceMetadataLabel",
    "new_uuid",
    "utcnow",
]
