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
"""Abstract interface for job data access."""

from abc import ABC, abstractmethod
from typing import Optional

from ingestor_orchestrator.models import CkanDataJob, JobStatus


class JobRepository(ABC):
    @abstractmethod
    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        resource_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        tags: Optional[str] = None,
        order_by: Optional[str] = None,
        order_dir: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[CkanDataJob], int]:
        """List jobs with filtering, ordering, and pagination.

        Returns (jobs, total_count).
        Does NOT eagerly load results — use get_job for that.
        """
        ...

    @abstractmethod
    async def get_job(self, job_id: str) -> Optional[CkanDataJob]:
        """Get a single job with results eagerly loaded."""
        ...

    @abstractmethod
    async def create_job(self, job: CkanDataJob) -> CkanDataJob:
        """Persist a new job."""
        ...

    @abstractmethod
    async def delete_job(self, job_id: str) -> bool:
        """Delete a job if it's pending or failed. Returns True if deleted."""
        ...
