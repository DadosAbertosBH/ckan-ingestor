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
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.api.jobs import _build_ckan_resource_url
from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.models import CkanDataJob, ResourceStatus
from ingestor_orchestrator.dto import (
    JobListResponse,
    JobResponse,
    ResourceDetailResponse,
    ResourceResponse,
)
from ingestor_orchestrator.repositories.resource_repository import ResourceRepository
from ingestor_orchestrator.repositories.sqlalchemy_resource_repository import (
    SqlAlchemyResourceRepository,
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


async def get_resource_repo(
    db: AsyncSession = Depends(get_db),
) -> ResourceRepository:
    """FastAPI dependency that provides a ResourceRepository."""
    return SqlAlchemyResourceRepository(db)


@router.get("/", response_model=list[ResourceResponse])
async def list_resources(
    status: Optional[ResourceStatus] = None,
    instance_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    repo: ResourceRepository = Depends(get_resource_repo),
):
    resources, labels_map, counts_map = await repo.list_resources(
        status=status,
        instance_id=instance_id,
        search=search,
        limit=limit,
        offset=offset,
    )

    if not resources:
        return []

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
    repo: ResourceRepository = Depends(get_resource_repo),
):
    detail = await repo.get_resource(resource_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Resource not found")

    # Build latest_job response
    latest_job_response = None
    if detail.latest_job:
        job = detail.latest_job
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
            labels=detail.labels,
            results=job.results,
        )

    # Build all jobs list responses
    job_list_responses = []
    for j in detail.all_jobs:
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
                labels=detail.labels,
            )
        )

    return ResourceDetailResponse(
        resource_id=detail.resource.resource_id,
        resource_name=detail.resource.resource_name,
        resource_url=detail.resource.resource_url,
        resource_format=detail.resource.resource_format,
        dataset_name=detail.resource.dataset_name,
        status=detail.resource.status,
        instance_id=detail.resource.instance_id,
        ckan_resource_url=_build_ckan_resource_url(
            detail.resource.instance.url,
            detail.resource.dataset_name,
            detail.resource.resource_id,
        )
        if detail.resource.instance
        else "",
        labels=detail.labels,
        job_count=len(detail.all_jobs),
        created_at=detail.resource.created_at,
        updated_at=detail.resource.updated_at,
        latest_job=latest_job_response,
        jobs=job_list_responses,
        preview=_extract_preview(detail.latest_job),
    )
