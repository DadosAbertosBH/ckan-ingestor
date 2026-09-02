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
import logging

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.services.metadata_service import MetadataService
from ingestor_orchestrator.services.sync_service import SyncService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/metadata", tags=["metadata"])


@router.post("/sync")
async def sync_all(
    db: AsyncSession = Depends(get_db),
):
    """Queue metadata syncs for all registered CKAN instances."""
    from sqlalchemy import select
    from ingestor_orchestrator.models import CkanInstance

    instances = (await db.execute(select(CkanInstance))).scalars().all()
    sync_service = SyncService(db)
    return [
        {"sync_id": (record := await sync_service.sync_metadata_for_instance(inst.id, inst.name, inst.url)).id,
         "instance_id": inst.id, "status": record.status}
        for inst in instances
    ]


@router.post("/sync/{instance_id}")
async def sync_instance(
    instance_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Queue an asynchronous metadata sync for a CKAN instance."""
    service = MetadataService(db)
    instance = await service.get_instance(instance_id)
    if not instance:
        return {"error": f"Instance {instance_id} not found"}

    sync_service = SyncService(db)
    sync_record = await sync_service.start_sync(instance.id)
    logger.info(f"Sync started for {instance.name} ({instance.url})...")
    try:
        sync_record = await sync_service.sync_metadata_for_instance(
            instance.id, instance.name, instance.url, sync_record
        )
    except Exception as e:
        logger.error(f"Sync failed for {instance.name}: {e}", exc_info=True)
        await sync_service.finish_sync(sync_record, {}, status="failure")
        return {"error": str(e)}
    return {"sync_id": sync_record.id, "instance_id": instance.id, "status": sync_record.status}
