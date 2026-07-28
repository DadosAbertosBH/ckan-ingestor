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
"""Test that metadata sync runs in a thread, not blocking the event loop."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


class FakeInstance:
    id = "inst-1"
    name = "Test"
    url = "https://test.example.com"


@pytest.mark.asyncio
async def test_sync_instance_dispatches_to_thread():
    """sync_metadata_for_instance must run via asyncio.to_thread, not directly."""
    from ingestor_orchestrator.api import metadata as api_module

    with (
        patch.object(api_module, "MetadataService") as mock_service_cls,
        patch.object(api_module, "sync_metadata_for_instance") as mock_sync,
        patch.object(api_module, "enqueue_outdated_resources",
                     new_callable=AsyncMock) as mock_enqueue,
    ):
        mock_service = AsyncMock()
        mock_service.get_instance.return_value = FakeInstance()
        mock_service.update_sync_result = AsyncMock()
        mock_service_cls.return_value = mock_service

        mock_sync.return_value = {"dataset_count": 1, "resource_count": 10}
        mock_enqueue.return_value = 5

        # Patch asyncio.to_thread to verify it's called
        with patch.object(asyncio, "to_thread") as mock_to_thread:
            mock_to_thread.return_value = {"dataset_count": 1, "resource_count": 10}

            result = await api_module.sync_instance("inst-1", AsyncMock())

        # Must dispatch blocking sync to a thread (not block the event loop)
        mock_to_thread.assert_called_once()
        # sync_metadata_for_instance must NOT be called directly
        mock_sync.assert_not_called()
        assert result["jobs_enqueued"] == 5


@pytest.mark.asyncio
async def test_sync_all_dispatches_to_thread():
    """sync_all_instances must run via asyncio.to_thread, not directly."""
    from ingestor_orchestrator.api import metadata as api_module

    with (
        patch.object(api_module, "sync_all_instances") as mock_sync_all,
    ):
        mock_sync_all.return_value = {"instances": 1}

        with patch.object(asyncio, "to_thread") as mock_to_thread:
            mock_to_thread.return_value = {"instances": 1}

            result = await api_module.sync_all(AsyncMock())

        mock_to_thread.assert_called_once()
        mock_sync_all.assert_not_called()
        assert result["instances"] == 1
