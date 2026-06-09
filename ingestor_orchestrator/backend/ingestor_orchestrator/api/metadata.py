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
from typing import Optional

from fastapi import APIRouter, Query

from ingestor_orchestrator.services.metadata_sync import (
    sync_all_instances,
    sync_metadata_for_instance,
)

router = APIRouter(prefix="/api/metadata", tags=["metadata"])


@router.post("/sync")
async def sync(instance_id: Optional[str] = Query(None)):
    """Sync CKAN datasets and resources metadata into DuckLake."""
    if instance_id:
        from sqlalchemy import select

        from ingestor_orchestrator.db import async_session
        from ingestor_orchestrator.models import CkanInstance

        async with async_session() as db:
            instance = (
                await db.execute(
                    select(CkanInstance).where(CkanInstance.id == instance_id)
                )
            ).scalar_one_or_none()
            if not instance:
                return {"error": f"Instance {instance_id} not found"}

            result = await asyncio.to_thread(
                sync_metadata_for_instance,
                instance_id=instance.id,
                instance_name=instance.name,
                instance_url=instance.url,
            )
            # Update MySQL instance metadata
            from datetime import datetime, timezone

            instance.last_metadata_synced = datetime.now(timezone.utc)
            instance.dataset_count = result.get("dataset_count", 0)
            instance.resource_count = result.get("resource_count", 0)
            await db.commit()
            return result

    result = await asyncio.to_thread(sync_all_instances)
    return result
