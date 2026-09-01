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
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select, asc as sa_asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import JobCreate, JobListResponse, JobResponse
from ingestor_orchestrator.duration_sort import duration_sort_key
from ingestor_orchestrator.models import (
    CkanDataJob,
    JobStatus,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.services.job_service import JobService

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


def _build_ckan_resource_url(
    instance_url: str, dataset_name: str, resource_id: str
) -> str:
    base = instance_url.rstrip("/")
    return f"{base}/dataset/{dataset_name}/resource/{resource_id}"


VALID_ORDER_FIELDS = {"created_at", "duration"}
VALID_ORDER_DIRS = {"asc", "desc"}


@router.get("/", response_model=list[JobListResponse])
async def list_jobs(
    status: Optional[JobStatus] = None,
    resource_id: Optional[str] = None,
    instance_id: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    order_by: Optional[str] = Query(None, description="Sort field: created_at or duration"),
    order_dir: Optional[str] = Query(None, description="Sort direction: asc or desc"),
    tags: Optional[str] = Query(None, description="Comma-separated tags to filter by"),
    db: AsyncSession = Depends(get_db),
):
    query = select(CkanDataJob).options(selectinload(CkanDataJob.instance))

    if status:
        query = query.where(CkanDataJob.status == status)
    if resource_id:
        query = query.where(CkanDataJob.resource_id == resource_id)
    if instance_id:
        query = query.where(CkanDataJob.instance_id == instance_id)

    # Filter by tags (OR semantics on ResourceMetadataLabel.label)
    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        if tag_list:
            matching_resource_ids_subq = (
                select(ResourceMetadataLabel.resource_id)
                .where(ResourceMetadataLabel.label.in_(tag_list))
                .distinct()
            )
            query = query.where(
                CkanDataJob.resource_id.in_(matching_resource_ids_subq)
            )

    # Apply ordering
    effective_order_by = order_by if order_by in VALID_ORDER_FIELDS else "created_at"
    effective_order_dir = order_dir if order_dir in VALID_ORDER_DIRS else "desc"

    if effective_order_by == "duration":
        # Duration ordering is done in Python for cross-DB compatibility.
        # Fetch all matching rows (before limit/offset), sort, then paginate.
        query = query.order_by(CkanDataJob.created_at.desc())
        result = await db.execute(query)
        jobs = result.scalars().all()

        # Sort by duration in Python
        reverse = effective_order_dir == "desc"
        jobs = sorted(jobs, key=lambda j: duration_sort_key(j, reverse))

        # Apply pagination after sort
        jobs = jobs[offset : offset + limit]
    else:
        query = query.order_by(
            sa_asc(CkanDataJob.created_at)
            if effective_order_dir == "asc"
            else desc(CkanDataJob.created_at)
        )
        query = query.limit(limit).offset(offset)
        result = await db.execute(query)
        jobs = result.scalars().all()

    # Fetch labels for these resource_ids
    resource_ids = list({j.resource_id for j in jobs})
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

    response_jobs = []
    for j in jobs:
        job_dict = {
            "id": j.id,
            "resource_id": j.resource_id,
            "resource_name": j.resource_name,
            "resource_url": j.resource_url,
            "resource_format": j.resource_format,
            "dataset_name": j.dataset_name,
            "status": j.status,
            "idempotency_key": j.idempotency_key,
            "instance_id": j.instance_id,
            "instance_name": j.instance.name if j.instance else None,
            "ckan_resource_url": _build_ckan_resource_url(
                j.instance.url, j.dataset_name, j.resource_id
            )
            if j.instance
            else "",
            "created_at": j.created_at,
            "updated_at": j.updated_at,
            "started_at": j.started_at,
            "completed_at": j.completed_at,
            "labels": labels_map.get(j.resource_id, []),
            "broker_type": j.broker_type,
            "message_stream": j.message_stream,
            "message_topic": j.message_topic,
            "message_partition": j.message_partition,
            "message_offset": j.message_offset,
        }
        response_jobs.append(JobListResponse(**job_dict))

    return response_jobs


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    query = (
        select(CkanDataJob)
        .options(selectinload(CkanDataJob.instance), selectinload(CkanDataJob.results))
        .where(CkanDataJob.id == job_id)
    )
    result = await db.execute(query)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Fetch labels for this job's resource
    label_rows = (
        (
            await db.execute(
                select(ResourceMetadataLabel).where(
                    ResourceMetadataLabel.resource_id == job.resource_id
                )
            )
        )
        .scalars()
        .all()
    )
    labels = [lbl.label for lbl in label_rows]

    return JobResponse(
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
        broker_type=job.broker_type,
        message_stream=job.message_stream,
        message_topic=job.message_topic,
        message_partition=job.message_partition,
        message_offset=job.message_offset,
    )


@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(data: JobCreate, db: AsyncSession = Depends(get_db)):
    service = JobService(db)
    job = await service.create_job(data)
    # Reload with instance relationship
    await db.refresh(job, attribute_names=["instance"])
    return JobResponse(
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
        labels=[],
        results=[],
        broker_type=job.broker_type,
        message_stream=job.message_stream,
        message_topic=job.message_topic,
        message_partition=job.message_partition,
        message_offset=job.message_offset,
    )


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(job_id: str, db: AsyncSession = Depends(get_db)):
    service = JobService(db)
    job = await service.retry_job(job_id)
    await db.refresh(job, attribute_names=["instance"])
    return JobResponse(
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
        labels=[],
        results=[],
        broker_type=job.broker_type,
        message_stream=job.message_stream,
        message_topic=job.message_topic,
        message_partition=job.message_partition,
        message_offset=job.message_offset,
    )


@router.delete("/{job_id}", status_code=204)
async def delete_job(job_id: str, db: AsyncSession = Depends(get_db)):
    job = await db.get(CkanDataJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.PENDING, JobStatus.FAILED):
        raise HTTPException(
            status_code=409, detail="Can only delete pending or failed jobs"
        )
    await db.delete(job)
    await db.commit()
