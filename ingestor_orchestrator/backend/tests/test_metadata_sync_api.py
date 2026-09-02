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
"""Tests for asynchronous metadata sync dispatch."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class FakeInstance:
    id = "inst-1"
    name = "Test"
    url = "https://test.example.com"


@pytest.mark.asyncio
async def test_sync_instance_queues_a_sync_record():
    """sync_instance publishes work and returns its pending sync id."""
    from ingestor_orchestrator.api import metadata as api_module

    with (
        patch.object(api_module, "MetadataService") as mock_service_cls,
        patch.object(api_module, "SyncService") as mock_sync_service_cls,
    ):
        mock_service = AsyncMock()
        mock_service.get_instance.return_value = FakeInstance()
        mock_service.update_sync_result = AsyncMock()
        mock_service_cls.return_value = mock_service

        mock_sync_service = AsyncMock()
        mock_sync_service.start_sync.return_value = MagicMock(id="sync-1", status="pending")
        mock_sync_service.sync_metadata_for_instance = AsyncMock(
            return_value=MagicMock(id="sync-1", status="pending")
        )
        mock_sync_service_cls.return_value = mock_sync_service

        result = await api_module.sync_instance("inst-1", AsyncMock())

        mock_sync_service.start_sync.assert_awaited_once()
        mock_sync_service.sync_metadata_for_instance.assert_awaited_once()
        mock_sync_service.finish_sync.assert_not_awaited()
        assert result == {"sync_id": "sync-1", "instance_id": "inst-1", "status": "pending"}
