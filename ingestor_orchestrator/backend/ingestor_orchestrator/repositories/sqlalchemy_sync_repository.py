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
"""SQLAlchemy implementation of SyncRepository."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ingestor_orchestrator.models import MetadataSync
from ingestor_orchestrator.repositories.sync_repository import SyncRepository


class SqlAlchemySyncRepository(SyncRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_sync(self, sync_id: str) -> MetadataSync | None:
        query = (
            select(MetadataSync)
            .options(selectinload(MetadataSync.instance))
            .where(MetadataSync.id == sync_id)
        )
        result = await self._session.execute(query)
        return result.scalar_one_or_none()

    async def list_syncs(
        self,
        instance_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[MetadataSync]:
        query = (
            select(MetadataSync)
            .options(selectinload(MetadataSync.instance))
            .order_by(MetadataSync.start_time.desc())
        )
        if instance_id:
            query = query.where(MetadataSync.instance_id == instance_id)
        query = query.limit(limit).offset(offset)
        result = await self._session.execute(query)
        return list(result.scalars().all())
