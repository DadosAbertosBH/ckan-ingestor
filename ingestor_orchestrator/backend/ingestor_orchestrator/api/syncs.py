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
        MetadataSyncResponse(
            id=s.id,
            instance_id=s.instance_id,
            instance_name=s.instance.name if s.instance else None,
            start_time=s.start_time,
            end_time=s.end_time,
            status=s.status,
            error_message=s.error_message,
            total_packages=s.total_packages,
            new_datasets=s.new_datasets,
            new_resources=s.new_resources,
            updated_datasets=s.updated_datasets,
            updated_resources=s.updated_resources,
        )
        for s in syncs
    ]
