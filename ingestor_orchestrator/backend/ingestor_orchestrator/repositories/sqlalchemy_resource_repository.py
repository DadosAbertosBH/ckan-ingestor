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
"""SQLAlchemy implementation of ResourceRepository."""

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ingestor_orchestrator.models import (
    CkanDataJob,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.repositories.resource_repository import (
    ResourceDetail,
    ResourceRepository,
)


class SqlAlchemyResourceRepository(ResourceRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def list_resources(
        self,
        status: JobStatus | None = None,
        instance_id: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[LatestResourceJob], dict[str, list[str]], dict[str, int]]:
        query = (
            select(LatestResourceJob)
            .options(selectinload(LatestResourceJob.instance))
            .order_by(LatestResourceJob.updated_at.desc())
        )

        if status:
            query = query.where(LatestResourceJob.status == status)
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
