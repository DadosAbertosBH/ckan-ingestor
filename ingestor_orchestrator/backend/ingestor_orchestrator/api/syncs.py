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

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import MetadataSyncResponse
from ingestor_orchestrator.repositories import (
    SqlAlchemySyncRepository,
    SyncRepository,
)


def get_sync_repository(
    db: AsyncSession = Depends(get_db),
) -> SyncRepository:
    return SqlAlchemySyncRepository(db)


router = APIRouter(prefix="/api/syncs", tags=["syncs"])


def sync_response(sync) -> MetadataSyncResponse:
    return MetadataSyncResponse(
        id=sync.id,
        instance_id=sync.instance_id,
        instance_name=sync.instance.name if sync.instance else None,
        start_time=sync.start_time,
        end_time=sync.end_time,
        status=sync.status,
        error_message=sync.error_message,
        total_packages=sync.total_packages,
        new_datasets=sync.new_datasets,
        new_resources=sync.new_resources,
        updated_datasets=sync.updated_datasets,
        updated_resources=sync.updated_resources,
    )


@router.get("/{sync_id}", response_model=MetadataSyncResponse)
async def get_sync(
    sync_id: str,
    repo: SyncRepository = Depends(get_sync_repository),
):
    """Return one metadata sync, including its terminal error when present."""
    sync = await repo.get_sync(sync_id)
    if sync is None:
        raise HTTPException(status_code=404, detail="Sync not found")
    return sync_response(sync)


@router.get("/", response_model=list[MetadataSyncResponse])
async def list_syncs(
    instance_id: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    repo: SyncRepository = Depends(get_sync_repository),
):
    """List metadata sync runs, most recent first."""
    syncs = await repo.list_syncs(instance_id=instance_id, limit=limit, offset=offset)

    return [
        sync_response(s)
        for s in syncs
    ]
