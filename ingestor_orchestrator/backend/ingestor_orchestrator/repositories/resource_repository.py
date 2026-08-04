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
"""Abstract interface for resource data access."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ingestor_orchestrator.models import CkanDataJob, JobStatus, LatestResourceJob


@dataclass
class ResourceDetail:
    """Result of get_resource — bundles ORM objects for the API layer."""

    resource: LatestResourceJob
    latest_job: CkanDataJob | None
    all_jobs: list[CkanDataJob] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)


class ResourceRepository(ABC):
    @abstractmethod
    async def list_resources(
        self,
        status: Optional[JobStatus] = None,
        instance_id: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[LatestResourceJob], dict[str, list[str]], dict[str, int]]:
        """List resources with filtering and pagination.

        Returns (resources, labels_map, counts_map) where:
        - resources: LatestResourceJob with instance eagerly loaded
        - labels_map: dict[resource_id -> list[label_value]]
        - counts_map: dict[resource_id -> job_count]
        """
        ...

    @abstractmethod
    async def get_resource(self, resource_id: str) -> Optional[ResourceDetail]:
        """Get full resource detail including latest job, all jobs, and labels.

        Returns None if resource does not exist.
        """
        ...
