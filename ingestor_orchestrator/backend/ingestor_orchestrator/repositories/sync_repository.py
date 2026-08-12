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
"""Abstract interface for metadata sync data access."""

from abc import ABC, abstractmethod

from ingestor_orchestrator.models import MetadataSync


class SyncRepository(ABC):
    @abstractmethod
    async def list_syncs(
        self,
        instance_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[MetadataSync]:
        """List metadata sync runs, most recent first.

        Optionally filter by instance_id. Supports pagination via limit/offset.
        Eagerly loads the related CkanInstance via selectinload.
        """
        ...
