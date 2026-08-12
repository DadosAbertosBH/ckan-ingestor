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
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.models import CkanInstance

logger = logging.getLogger(__name__)


class MetadataService:
    """Service layer for CKAN metadata operations — keeps data access out of controllers."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_instance(self, instance_id: str) -> CkanInstance | None:
        return await self.db.get(CkanInstance, instance_id)

    async def update_sync_result(
        self, instance: CkanInstance, dataset_count: int, resource_count: int
    ) -> None:
        instance.last_metadata_synced = datetime.now(timezone.utc)
        instance.dataset_count = dataset_count
        instance.resource_count = resource_count
        await self.db.commit()
