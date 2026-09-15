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
"""Tests for job publication through the Iggy message bus."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from ingestor_orchestrator.services.job_service import JobService

_FACTORY_PATH = "ingestor_orchestrator.iggy_queue.get_iggy_bus"


def _mock_bus():
    mock = AsyncMock()
    return mock


@pytest.mark.asyncio
async def test_publish_sends_job_id_and_ckan_url():
    """Producer sends a message with job_id and ckan_url."""
    bus = _mock_bus()

    with patch(_FACTORY_PATH, return_value=bus):
        service = JobService(AsyncMock())
        await service._publish_job(
            "test-job", "resource-1", ckan_url="https://dados.pbh.gov.br"
        )

    bus.publish.assert_awaited_once()
    args, kwargs = bus.publish.await_args
    assert args[0] == "jobs"
    assert kwargs["key"] == "resource-1"
    payload = json.loads(args[1].decode())
    assert payload["job_id"] == "test-job"
    assert payload["resource_id"] == "resource-1"
    assert payload["ckan_url"] == "https://dados.pbh.gov.br"
    assert "csv_delimiter" not in payload


@pytest.mark.asyncio
async def test_publish_retry_uses_retry_topic():
    """retry=True publishes to the retry topic."""
    bus = _mock_bus()

    with patch(_FACTORY_PATH, return_value=bus):
        service = JobService(AsyncMock())
        await service._publish_job("retry-job", "resource-1", retry=True)

    args, _ = bus.publish.await_args
    assert args[0] == "jobs-retry"


@pytest.mark.asyncio
async def test_publish_includes_optional_csv_delimiter():
    bus = _mock_bus()

    with patch(_FACTORY_PATH, return_value=bus):
        service = JobService(AsyncMock())
        await service._publish_job("test-job", "resource-1", csv_delimiter=";")

    args, _kwargs = bus.publish.await_args
    payload = json.loads(args[1].decode())
    assert payload["csv_delimiter"] == ";"


@pytest.mark.asyncio
async def test_publish_includes_datastore_status_from_metadata():
    bus = _mock_bus()

    with patch(_FACTORY_PATH, return_value=bus):
        service = JobService(AsyncMock())
        await service._publish_job("test-job", "resource-1", datastore_active=True)

    args, _kwargs = bus.publish.await_args
    payload = json.loads(args[1].decode())
    assert payload["datastore_active"] is True
