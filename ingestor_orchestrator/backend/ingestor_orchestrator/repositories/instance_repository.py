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
"""Abstract interface for CKAN instance data access."""

from abc import ABC, abstractmethod

from ingestor_orchestrator.models import CkanInstance


class InstanceRepository(ABC):
    @abstractmethod
    async def list_instances(self) -> list[CkanInstance]:
        """Return all instances ordered by name."""
        ...

    @abstractmethod
    async def get_instance(self, instance_id: str) -> CkanInstance | None:
        """Return a single instance by ID, or None if not found."""
        ...

    @abstractmethod
    async def create_instance(self, instance: CkanInstance) -> CkanInstance:
        """Persist a new instance and return it."""
        ...

    @abstractmethod
    async def delete_instance(self, instance_id: str) -> bool:
        """Delete an instance by ID. Returns True if deleted, False if not found."""
        ...

    @abstractmethod
    async def get_instances_by_ids(self, instance_ids: list[str]) -> list[CkanInstance]:
        """Return instances matching the given IDs. Used by datasets API."""
        ...
