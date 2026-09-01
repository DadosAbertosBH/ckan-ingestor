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

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import DatasetResponse
from ingestor_orchestrator.models import (
    CkanInstance,
    LatestResourceJob,
    ResourceMetadataLabel,
)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


@router.get("/", response_model=list[DatasetResponse])
async def list_datasets(
    instance_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List datasets aggregated from latest resource jobs.

    Groups resources by instance_id + dataset_name, with counts per status.
    """
    # Subquery: aggregate LatestResourceJob by instance_id + dataset_name
    agg_subq = (
        select(
            LatestResourceJob.instance_id,
            LatestResourceJob.dataset_name,
            func.count(LatestResourceJob.resource_id).label("total_resources"),
            func.sum(case((LatestResourceJob.status == "pending", 1), else_=0)).label(
                "pending_resources"
            ),
            func.sum(
                case((LatestResourceJob.status == "processing", 1), else_=0)
            ).label("processing_resources"),
            func.sum(case((LatestResourceJob.status == "completed", 1), else_=0)).label(
                "completed_resources"
            ),
            func.sum(case((LatestResourceJob.status == "failed", 1), else_=0)).label(
                "failed_resources"
            ),
            func.sum(case((LatestResourceJob.status == "outdated", 1), else_=0)).label(
                "outdated_resources"
            ),
            func.sum(case((ResourceMetadataLabel.label == "empty", 1), else_=0)).label(
                "empty_resources"
            ),
            func.max(LatestResourceJob.updated_at).label("updated_at"),
        )
        .outerjoin(
            ResourceMetadataLabel,
            (LatestResourceJob.resource_id == ResourceMetadataLabel.resource_id)
            & (ResourceMetadataLabel.label == "empty"),
        )
        .group_by(LatestResourceJob.instance_id, LatestResourceJob.dataset_name)
    )

    if instance_id:
        agg_subq = agg_subq.where(LatestResourceJob.instance_id == instance_id)
    if search:
        agg_subq = agg_subq.where(LatestResourceJob.dataset_name.ilike(f"%{search}%"))

    agg_subq = agg_subq.order_by(func.max(LatestResourceJob.updated_at).desc())
    agg_subq = agg_subq.limit(limit).offset(offset)

    agg_result = await db.execute(agg_subq)
    agg_rows = agg_result.all()

    if not agg_rows:
        return []

    # Fetch instance names and URLs in one query
    instance_ids = {row.instance_id for row in agg_rows}
    instance_query = select(CkanInstance).where(CkanInstance.id.in_(instance_ids))
    inst_result = await db.execute(instance_query)
    instances = inst_result.scalars().all()
    instance_info_map = {
        inst.id: {
            "name": inst.name,
            "url": inst.url,
            "last_synced": inst.last_metadata_synced,
        }
        for inst in instances
    }

    return [
        DatasetResponse(
            instance_id=row.instance_id,
            instance_name=instance_info_map.get(row.instance_id, {}).get("name"),
            dataset_name=row.dataset_name,
            ckan_dataset_url=_build_ckan_dataset_url(
                instance_info_map.get(row.instance_id, {}).get("url", ""),
                row.dataset_name,
            ),
            total_resources=row.total_resources,
            pending_resources=row.pending_resources,
            processing_resources=row.processing_resources,
            completed_resources=row.completed_resources,
            failed_resources=row.failed_resources,
            outdated_resources=row.outdated_resources,
            empty_resources=row.empty_resources,
            updated_at=row.updated_at,
            instance_last_synced_at=instance_info_map.get(row.instance_id, {}).get(
                "last_synced"
            ),
        )
        for row in agg_rows
    ]


def _build_ckan_dataset_url(instance_url: str, dataset_name: str) -> str:
    if not instance_url:
        return ""
    base = instance_url.rstrip("/")
    return f"{base}/dataset/{dataset_name}"
