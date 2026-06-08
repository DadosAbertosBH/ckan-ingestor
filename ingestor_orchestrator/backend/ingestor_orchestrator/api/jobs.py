from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.models import CkanDataJob, JobStatus
from ingestor_orchestrator.schemas import JobCreate, JobListResponse, JobResponse
from ingestor_orchestrator.services.job_service import JobService

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@router.get("/", response_model=list[JobListResponse])
async def list_jobs(
    status: Optional[JobStatus] = None,
    resource_id: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    query = select(CkanDataJob).order_by(CkanDataJob.created_at.desc())
    if status:
        query = query.where(CkanDataJob.status == status)
    if resource_id:
        query = query.where(CkanDataJob.resource_id == resource_id)
    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    job = await db.get(CkanDataJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(data: JobCreate, db: AsyncSession = Depends(get_db)):
    service = JobService(db)
    job = await service.create_job(data)
    return job


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(job_id: str, db: AsyncSession = Depends(get_db)):
    service = JobService(db)
    job = await service.retry_job(job_id)
    return job


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
