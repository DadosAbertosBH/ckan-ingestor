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
"""SQLAlchemy implementation of InstanceRepository."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.models import CkanInstance
from ingestor_orchestrator.repositories.instance_repository import InstanceRepository


class SqlAlchemyInstanceRepository(InstanceRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def list_instances(self) -> list[CkanInstance]:
        query = select(CkanInstance).order_by(CkanInstance.name)
        result = await self._session.execute(query)
        return list(result.scalars().all())

    async def get_instance(self, instance_id: str) -> CkanInstance | None:
        return await self._session.get(CkanInstance, instance_id)

    async def create_instance(self, instance: CkanInstance) -> CkanInstance:
        self._session.add(instance)
        await self._session.flush()
        return instance

    async def delete_instance(self, instance_id: str) -> bool:
        instance = await self._session.get(CkanInstance, instance_id)
        if not instance:
            return False
        await self._session.delete(instance)
        await self._session.flush()
        return True

    async def get_instances_by_ids(
        self, instance_ids: list[str]
    ) -> list[CkanInstance]:
        query = select(CkanInstance).where(CkanInstance.id.in_(instance_ids))
        result = await self._session.execute(query)
        return list(result.scalars().all())
