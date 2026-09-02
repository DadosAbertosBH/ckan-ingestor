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
import json
import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.models import MetadataSync

logger = logging.getLogger(__name__)


class SyncService:
    """Service layer for CKAN metadata syncs — records runs and syncs DuckLake."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def start_sync(self, instance_id: str) -> MetadataSync:
        """Create a sync record with start_time set to now."""
        record = MetadataSync(instance_id=instance_id, status="pending")
        self.db.add(record)
        await self.db.commit()
        await self.db.refresh(record)
        return record

    async def finish_sync(
        self, record: MetadataSync, result: dict, status: str = "success"
    ) -> MetadataSync:
        """Fill in end_time, status, and the new/updated counts for a finished sync."""
        record.end_time = datetime.now(timezone.utc)
        record.status = status
        record.error_message = (
            str(result["error_message"])[:16_000]
            if status == "failure" and result.get("error_message")
            else None
        )
        record.total_packages = result.get("total_packages", 0)
        record.new_datasets = result.get("new_datasets", 0)
        record.new_resources = result.get("new_resources", 0)
        record.updated_datasets = result.get("updated_datasets", 0)
        record.updated_resources = result.get("updated_resources", 0)
        await self.db.commit()
        await self.db.refresh(record)
        message = (
            f"Sync record {record.id} saved: status={record.status}, "
            f"total={record.total_packages}, new_ds={record.new_datasets}, "
            f"new_rs={record.new_resources}, upd_ds={record.updated_datasets}, "
            f"upd_rs={record.updated_resources}"
        )
        if status == "failure":
            logger.error("%s, error=%s", message, result.get("error_message", "unknown"))
        else:
            logger.info(message)
        return record

    async def sync_metadata_for_instance(
        self,
        instance_id: str,
        instance_name: str,
        instance_url: str,
        sync_record: MetadataSync | None = None,
    ) -> MetadataSync:
        """Create and publish an asynchronous metadata sync command."""
        from ingestor_orchestrator.config import settings
        from ingestor_orchestrator.iggy_queue import get_iggy_bus

        record = sync_record or await self.start_sync(instance_id)
        payload = json.dumps(
            {
                "sync_id": record.id,
                "instance_id": instance_id,
                "instance_name": instance_name,
                "instance_url": instance_url,
            }
        ).encode()
        await get_iggy_bus().publish(
            settings.iggy_metadata_sync_topic, payload, key=record.id
        )
        return record
