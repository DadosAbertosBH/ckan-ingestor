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
import asyncio
import logging

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.services.metadata_service import MetadataService
from ingestor_orchestrator.services.metadata_sync import (
    enqueue_outdated_resources,
    sync_all_instances,
)
from ingestor_orchestrator.services.sync_service import SyncService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/metadata", tags=["metadata"])


@router.post("/sync")
async def sync_all(
    db: AsyncSession = Depends(get_db),
):
    """Sync metadata for all CKAN instances."""
    logger.info("Sync started for all instances...")
    try:
        result = await asyncio.to_thread(sync_all_instances)
    except Exception as e:
        logger.error(f"Sync all failed: {e}", exc_info=True)
        return {"error": str(e)}
    logger.info("Sync all instances complete")
    return result


@router.post("/sync/{instance_id}")
async def sync_instance(
    instance_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Sync metadata for a specific CKAN instance and enqueue outdated resources."""
    service = MetadataService(db)
    instance = await service.get_instance(instance_id)
    if not instance:
        return {"error": f"Instance {instance_id} not found"}

    sync_service = SyncService(db)
    sync_record = await sync_service.start_sync(instance.id)

    logger.info(f"Sync started for {instance.name} ({instance.url})...")
    try:
        result = await sync_service.sync_metadata_for_instance(
            instance.id, instance.name, instance.url
        )
    except Exception as e:
        logger.error(f"Sync failed for {instance.name}: {e}", exc_info=True)
        await sync_service.finish_sync(sync_record, {"error": str(e)}, status="failure")
        return {"error": str(e)}

    dataset_count = result.get("dataset_count", 0)
    resource_count = result.get("resource_count", 0)

    try:
        enqueued = await enqueue_outdated_resources(instance.id, instance.name)
    except Exception as e:
        logger.error(f"Enqueue failed for {instance.name}: {e}", exc_info=True)
        enqueued = 0
    await service.update_sync_result(instance, dataset_count, resource_count)
    # result = {}
    # enqueued = 0
    # dataset_count = 0
    # resource_count = 0

    result["jobs_enqueued"] = enqueued
    await sync_service.finish_sync(sync_record, result)
    logger.info(
        f"Sync done for {instance.name}: "
        f"{dataset_count} datasets, {resource_count} resources, "
        f"{enqueued} jobs enqueued"
    )
    return result
