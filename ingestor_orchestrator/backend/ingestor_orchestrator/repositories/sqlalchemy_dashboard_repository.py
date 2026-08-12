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
"""SQLAlchemy implementation of DashboardRepository."""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.dto import CkanInstanceResponse, InstanceStats
from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanInstance,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.repositories.dashboard_repository import DashboardRepository


class SqlAlchemyDashboardRepository(DashboardRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_stats(self) -> list[InstanceStats]:
        # Fetch all instances
        instance_result = await self._session.execute(
            select(CkanInstance).order_by(CkanInstance.name)
        )
        instances = list(instance_result.scalars().all())

        if not instances:
            return []

        # Fetch job counts grouped by instance_id and status
        job_counts = await self._session.execute(
            select(
                CkanDataJob.instance_id,
                CkanDataJob.status,
                func.count(CkanDataJob.id),
            ).group_by(CkanDataJob.instance_id, CkanDataJob.status)
        )
        counts_by_instance: dict[str, dict[JobStatus, int]] = {}
        for row in job_counts:
            inst_id, status, count = row
            counts_by_instance.setdefault(inst_id, {})[status] = count

        # Fetch empty resource counts grouped by instance_id
        empty_counts_result = await self._session.execute(
            select(
                LatestResourceJob.instance_id,
                func.count().label("empty_count"),
            )
            .join(
                ResourceMetadataLabel,
                (LatestResourceJob.resource_id == ResourceMetadataLabel.resource_id)
                & (ResourceMetadataLabel.label == "empty"),
            )
            .group_by(LatestResourceJob.instance_id)
        )
        empty_counts: dict[str, int] = {
            row.instance_id: row.empty_count for row in empty_counts_result
        }

        result = []
        for inst in instances:
            counts = counts_by_instance.get(inst.id, {})
            result.append(
                InstanceStats(
                    instance=CkanInstanceResponse.model_validate(inst),
                    pending=counts.get(JobStatus.PENDING, 0),
                    processing=counts.get(JobStatus.PROCESSING, 0),
                    completed=counts.get(JobStatus.COMPLETED, 0),
                    failed=counts.get(JobStatus.FAILED, 0),
                    empty=empty_counts.get(inst.id, 0),
                )
            )

        return result
