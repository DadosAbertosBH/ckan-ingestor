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

from fastapi import APIRouter

from ingestor_orchestrator.services.metadata_sync import sync_metadata

router = APIRouter(prefix="/api/metadata", tags=["metadata"])


@router.post("/sync")
async def sync():
    """Sync CKAN datasets and resources metadata into DuckLake."""
    result = await asyncio.to_thread(sync_metadata)
    return result
