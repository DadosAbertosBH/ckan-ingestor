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
"""SQLAlchemy implementation of JobRepository."""

from sqlalchemy import desc, func, select, asc as sa_asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ingestor_orchestrator.duration_sort import duration_sort_key
from ingestor_orchestrator.models import (
    CkanDataJob,
    JobStatus,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.repositories.job_repository import JobRepository

VALID_ORDER_FIELDS = {"created_at", "duration"}
VALID_ORDER_DIRS = {"asc", "desc"}


class SqlAlchemyJobRepository(JobRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def list_jobs(
        self,
        status: JobStatus | None = None,
        resource_id: str | None = None,
        instance_id: str | None = None,
        tags: str | None = None,
        order_by: str | None = None,
        order_dir: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[CkanDataJob], int]:
        query = select(CkanDataJob).options(selectinload(CkanDataJob.instance))

        if status:
            query = query.where(CkanDataJob.status == status)
        if resource_id:
            query = query.where(CkanDataJob.resource_id == resource_id)
        if instance_id:
            query = query.where(CkanDataJob.instance_id == instance_id)

        if tags:
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]
            if tag_list:
                matching = (
                    select(ResourceMetadataLabel.resource_id)
                    .where(ResourceMetadataLabel.label.in_(tag_list))
                    .distinct()
                )
                query = query.where(CkanDataJob.resource_id.in_(matching))

        # Count total
        count_q = select(func.count()).select_from(query.subquery())
        total = (await self._session.execute(count_q)).scalar() or 0

        # Ordering
        field = order_by if order_by in VALID_ORDER_FIELDS else "created_at"
        direction = order_dir if order_dir in VALID_ORDER_DIRS else "desc"

        if field == "duration":
            query = query.order_by(CkanDataJob.created_at.desc())
            result = await self._session.execute(query)
            jobs = list(result.scalars().all())
            rev = direction == "desc"
            jobs.sort(key=lambda j: duration_sort_key(j, rev))
            jobs = jobs[offset : offset + limit]
        else:
            query = query.order_by(
                sa_asc(CkanDataJob.created_at)
                if direction == "asc"
                else desc(CkanDataJob.created_at)
            )
            query = query.limit(limit).offset(offset)
            result = await self._session.execute(query)
            jobs = list(result.scalars().all())

        return jobs, total

    async def get_job(self, job_id: str) -> CkanDataJob | None:
        query = (
            select(CkanDataJob)
            .options(
                selectinload(CkanDataJob.instance),
                selectinload(CkanDataJob.results),
            )
            .where(CkanDataJob.id == job_id)
        )
        result = await self._session.execute(query)
        return result.scalar_one_or_none()

    async def create_job(self, job: CkanDataJob) -> CkanDataJob:
        self._session.add(job)
        await self._session.flush()
        return job

    async def delete_job(self, job_id: str) -> bool:
        job = await self._session.get(CkanDataJob, job_id)
        if not job:
            return False
        if job.status not in (JobStatus.PENDING, JobStatus.FAILED):
            return False
        await self._session.delete(job)
        await self._session.commit()
        return True
