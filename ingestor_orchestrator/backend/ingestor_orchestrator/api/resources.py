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
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ingestor_orchestrator.api.jobs import _build_ckan_resource_url
from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.models import (
    CkanDataJob,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.dto import (
    JobListResponse,
    JobResponse,
    ResourceDetailResponse,
    ResourceResponse,
)

router = APIRouter(prefix="/api/resources", tags=["resources"])


def _extract_preview(job: CkanDataJob | None) -> list[dict]:
    """Extract dataset_preview from the latest successful result."""
    if not job or not job.results:
        return []
    for result in job.results:
        if result.success and result.dataset_preview:
            preview = result.dataset_preview
            if isinstance(preview, list):
                return preview
    return []


@router.get("/", response_model=list[ResourceResponse])
async def list_resources(
    status: Optional[JobStatus] = None,
    instance_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
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
    result = await db.execute(query)
    resources = result.scalars().all()

    if not resources:
        return []

    resource_ids = [r.resource_id for r in resources]

    # Fetch labels
    labels_map: dict[str, list[str]] = {}
    if resource_ids:
        label_rows = (
            (
                await db.execute(
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
    job_counts = await db.execute(
        select(CkanDataJob.resource_id, func.count(CkanDataJob.id))
        .where(CkanDataJob.resource_id.in_(resource_ids))
        .group_by(CkanDataJob.resource_id)
    )
    counts_map = dict(job_counts.all())

    response_list = []
    for r in resources:
        response_list.append(
            ResourceResponse(
                resource_id=r.resource_id,
                resource_name=r.resource_name,
                resource_url=r.resource_url,
                resource_format=r.resource_format,
                dataset_name=r.dataset_name,
                status=r.status,
                instance_id=r.instance_id,
                ckan_resource_url=_build_ckan_resource_url(
                    r.instance.url, r.dataset_name, r.resource_id
                )
                if r.instance
                else "",
                labels=labels_map.get(r.resource_id, []),
                job_count=counts_map.get(r.resource_id, 0),
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
        )

    return response_list


@router.get("/{resource_id}", response_model=ResourceDetailResponse)
async def get_resource(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
):
    latest = await db.get(LatestResourceJob, resource_id)
    if not latest:
        raise HTTPException(status_code=404, detail="Resource not found")

    # Load instance relationship
    await db.refresh(latest, attribute_names=["instance"])

    # Get latest job details
    job_query = (
        select(CkanDataJob)
        .options(selectinload(CkanDataJob.instance))
        .where(CkanDataJob.id == latest.latest_job_id)
    )
    job_result = await db.execute(job_query)
    job = job_result.scalar_one_or_none()

    # Get all jobs for this resource
    all_jobs_query = (
        select(CkanDataJob)
        .options(selectinload(CkanDataJob.instance))
        .where(CkanDataJob.resource_id == resource_id)
        .order_by(CkanDataJob.created_at.desc())
    )
    all_jobs_result = await db.execute(all_jobs_query)
    all_jobs = all_jobs_result.scalars().all()

    # Fetch labels
    label_rows = (
        (
            await db.execute(
                select(ResourceMetadataLabel).where(
                    ResourceMetadataLabel.resource_id == resource_id
                )
            )
        )
        .scalars()
        .all()
    )
    labels = [lbl.label for lbl in label_rows]

    # Job count
    job_count = len(all_jobs)

    # Build latest_job response
    latest_job_response = None
    if job:
        latest_job_response = JobResponse(
            id=job.id,
            resource_id=job.resource_id,
            resource_name=job.resource_name,
            resource_url=job.resource_url,
            resource_format=job.resource_format,
            dataset_name=job.dataset_name,
            status=job.status,
            idempotency_key=job.idempotency_key,
            instance_id=job.instance_id,
            ckan_resource_url=_build_ckan_resource_url(
                job.instance.url, job.dataset_name, job.resource_id
            )
            if job.instance
            else "",
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            labels=labels,
            results=job.results,
        )

    # Build all jobs list responses
    job_list_responses = []
    for j in all_jobs:
        job_list_responses.append(
            JobListResponse(
                id=j.id,
                resource_id=j.resource_id,
                resource_name=j.resource_name,
                resource_url=j.resource_url,
                resource_format=j.resource_format,
                dataset_name=j.dataset_name,
                status=j.status,
                idempotency_key=j.idempotency_key,
                instance_id=j.instance_id,
                ckan_resource_url=_build_ckan_resource_url(
                    j.instance.url, j.dataset_name, j.resource_id
                )
                if j.instance
                else "",
                created_at=j.created_at,
                updated_at=j.updated_at,
                started_at=j.started_at,
                completed_at=j.completed_at,
                labels=labels,
            )
        )

    return ResourceDetailResponse(
        resource_id=latest.resource_id,
        resource_name=latest.resource_name,
        resource_url=latest.resource_url,
        resource_format=latest.resource_format,
        dataset_name=latest.dataset_name,
        status=latest.status,
        instance_id=latest.instance_id,
        ckan_resource_url=_build_ckan_resource_url(
            latest.instance.url, latest.dataset_name, latest.resource_id
        )
        if latest.instance
        else "",
        labels=labels,
        job_count=job_count,
        created_at=latest.created_at,
        updated_at=latest.updated_at,
        latest_job=latest_job_response,
        jobs=job_list_responses,
        preview=_extract_preview(job),
    )
