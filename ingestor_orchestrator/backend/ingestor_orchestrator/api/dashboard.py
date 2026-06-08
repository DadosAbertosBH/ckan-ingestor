from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.models import CkanDataJob, JobStatus
from ingestor_orchestrator.schemas import DashboardStats

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=DashboardStats)
async def get_stats(db: AsyncSession = Depends(get_db)):
    total = (await db.execute(select(func.count(CkanDataJob.id)))).scalar() or 0
    pending = (
        await db.execute(
            select(func.count(CkanDataJob.id)).where(
                CkanDataJob.status == JobStatus.PENDING
            )
        )
    ).scalar() or 0
    processing = (
        await db.execute(
            select(func.count(CkanDataJob.id)).where(
                CkanDataJob.status == JobStatus.PROCESSING
            )
        )
    ).scalar() or 0
    completed = (
        await db.execute(
            select(func.count(CkanDataJob.id)).where(
                CkanDataJob.status == JobStatus.COMPLETED
            )
        )
    ).scalar() or 0
    failed = (
        await db.execute(
            select(func.count(CkanDataJob.id)).where(
                CkanDataJob.status == JobStatus.FAILED
            )
        )
    ).scalar() or 0
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
    last_24h = (
        await db.execute(
            select(func.count(CkanDataJob.id)).where(
                CkanDataJob.created_at >= yesterday
            )
        )
    ).scalar() or 0

    return DashboardStats(
        total_jobs=total,
        pending=pending,
        processing=processing,
        completed=completed,
        failed=failed,
        last_24h=last_24h,
    )
