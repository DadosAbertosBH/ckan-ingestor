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
"""SQLAlchemy implementation of ResourceRepository."""

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ingestor_orchestrator.models import (
    CkanDataJob,
    LatestResourceJob,
    LastTerminalStatus,
    ResourceMetadataLabel,
    ResourceStatus,
)
from ingestor_orchestrator.repositories.resource_repository import (
    ResourceDetail,
    ResourceRepository,
)
from ingestor_orchestrator.resource_status import classify_resource_status


class SqlAlchemyResourceRepository(ResourceRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def list_resources(
        self,
        status: ResourceStatus | None = None,
        instance_id: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[LatestResourceJob], dict[str, list[str]], dict[str, int]]:
        query = (
            select(LatestResourceJob)
            .options(selectinload(LatestResourceJob.instance))
            .join(CkanDataJob, CkanDataJob.id == LatestResourceJob.latest_job_id)
            .outerjoin(
                LastTerminalStatus,
                LastTerminalStatus.resource_id == LatestResourceJob.resource_id,
            )
            .order_by(LatestResourceJob.updated_at.desc())
        )

        if status:
            status_filters = {
                ResourceStatus.PENDING: and_(
                    CkanDataJob.status == "pending",
                    LastTerminalStatus.resource_id.is_(None),
                ),
                ResourceStatus.PROCESSING: CkanDataJob.status == "processing",
                ResourceStatus.COMPLETED: CkanDataJob.status == "completed",
                ResourceStatus.FAILED: or_(
                    CkanDataJob.status == "failed",
                    and_(
                        CkanDataJob.status == "pending",
                        LastTerminalStatus.last_terminal_status == "failed",
                    ),
                ),
                ResourceStatus.OUTDATED: and_(
                    CkanDataJob.status == "pending",
                    LastTerminalStatus.last_terminal_status == "completed",
                ),
            }
            query = query.where(status_filters[status])
        if instance_id:
            query = query.where(LatestResourceJob.instance_id == instance_id)
        if search:
            query = query.where(
                or_(
                    LatestResourceJob.resource_name.ilike(f"%{search}%"),
                    LatestResourceJob.dataset_name.ilike(f"%{search}%"),
                )
            )

        query = query.limit(limit).offset(offset)
        result = await self._session.execute(query)
        resources = list(result.scalars().all())

        if not resources:
            return [], {}, {}

        await self._apply_operational_status(resources)

        resource_ids = [r.resource_id for r in resources]

        # Fetch labels
        labels_map: dict[str, list[str]] = {}
        label_rows = (
            (
                await self._session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id.in_(resource_ids)
                    )
                )
            )
            .scalars()
            .all()
        )
        for lbl in label_rows:
            labels_map.setdefault(lbl.resource_id, []).append(lbl.label)

        # Count jobs per resource
        job_counts = await self._session.execute(
            select(CkanDataJob.resource_id, func.count(CkanDataJob.id))
            .where(CkanDataJob.resource_id.in_(resource_ids))
            .group_by(CkanDataJob.resource_id)
        )
        counts_map = dict(job_counts.all())

        return resources, labels_map, counts_map

    async def get_resource(self, resource_id: str) -> ResourceDetail | None:
        latest = await self._session.get(LatestResourceJob, resource_id)
        if not latest:
            return None

        # Load instance relationship
        await self._session.refresh(latest, attribute_names=["instance"])

        # Get latest job with results eagerly loaded
        job_query = (
            select(CkanDataJob)
            .options(
                selectinload(CkanDataJob.instance),
                selectinload(CkanDataJob.results),
            )
            .where(CkanDataJob.id == latest.latest_job_id)
        )
        job_result = await self._session.execute(job_query)
        latest_job = job_result.scalar_one_or_none()
        await self._apply_operational_status([latest])

        # Get all jobs for this resource
        all_jobs_query = (
            select(CkanDataJob)
            .options(selectinload(CkanDataJob.instance))
            .where(CkanDataJob.resource_id == resource_id)
            .order_by(CkanDataJob.created_at.desc())
        )
        all_jobs_result = await self._session.execute(all_jobs_query)
        all_jobs = list(all_jobs_result.scalars().all())

        # Fetch labels
        label_rows = (
            (
                await self._session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == resource_id
                    )
                )
            )
            .scalars()
            .all()
        )
        labels = [lbl.label for lbl in label_rows]

        return ResourceDetail(
            resource=latest,
            latest_job=latest_job,
            all_jobs=all_jobs,
            labels=labels,
        )

    async def _apply_operational_status(
        self, resources: list[LatestResourceJob]
    ) -> None:
        """Keep resource responses correct for rows created before the projection."""
        job_ids = [resource.latest_job_id for resource in resources]
        resource_ids = [resource.resource_id for resource in resources]
        jobs = (
            await self._session.execute(
                select(CkanDataJob).where(CkanDataJob.id.in_(job_ids))
            )
        ).scalars().all()
        jobs_by_id = {job.id: job for job in jobs}
        terminals = (
            await self._session.execute(
                select(LastTerminalStatus).where(
                    LastTerminalStatus.resource_id.in_(resource_ids)
                )
            )
        ).scalars().all()
        terminals_by_resource = {row.resource_id: row for row in terminals}
        for resource in resources:
            job = jobs_by_id.get(resource.latest_job_id)
            terminal = terminals_by_resource.get(resource.resource_id)
            if job:
                resource.status = classify_resource_status(
                    job.status,
                    terminal.last_terminal_status if terminal else None,
                )
