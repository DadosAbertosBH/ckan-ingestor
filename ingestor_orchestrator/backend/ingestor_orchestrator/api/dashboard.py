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
from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import CkanInstanceResponse, InstanceStats
from ingestor_orchestrator.models import CkanDataJob, CkanInstance, JobStatus

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=list[InstanceStats])
async def get_stats(db: AsyncSession = Depends(get_db)):
    # Fetch all instances
    instance_result = await db.execute(select(CkanInstance).order_by(CkanInstance.name))
    instances = instance_result.scalars().all()

    # Fetch job counts grouped by instance_id and status
    job_counts = await db.execute(
        select(
            CkanDataJob.instance_id,
            CkanDataJob.status,
            func.count(CkanDataJob.id),
        ).group_by(CkanDataJob.instance_id, CkanDataJob.status)
    )
    # Build a lookup: {instance_id: {status: count}}
    counts_by_instance: dict[str, dict[JobStatus, int]] = {}
    for row in job_counts:
        inst_id, status, count = row
        counts_by_instance.setdefault(inst_id, {})[status] = count

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
            )
        )

    return result
