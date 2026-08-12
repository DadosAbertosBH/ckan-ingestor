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
"""Test Kafka producer — uses kafka-python's send().get() API."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ingestor_orchestrator.services.job_service import JobService

_FACTORY_PATH = "ingestor_orchestrator.kafka.get_kafka_producer"


def _mock_producer():
    mock = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = None
    mock.send.return_value = mock_future
    return mock


@pytest.mark.asyncio
async def test_publish_sends_job_id_and_ckan_url():
    """Producer sends a message with job_id and ckan_url."""
    producer = _mock_producer()

    with patch(_FACTORY_PATH, return_value=producer):
        service = JobService(AsyncMock())
        await service._publish_job(
            "test-job", "resource-1", ckan_url="https://dados.pbh.gov.br"
        )

    producer.send.assert_called_once()
    args, _kwargs = producer.send.call_args
    assert args[0] == "ckan.ingest.jobs"
    payload = json.loads(args[1].decode())
    assert payload["job_id"] == "test-job"
    assert payload["resource_id"] == "resource-1"
    assert payload["ckan_url"] == "https://dados.pbh.gov.br"


@pytest.mark.asyncio
async def test_publish_retry_uses_retry_topic():
    """retry=True publishes to the retry topic."""
    producer = _mock_producer()

    with patch(_FACTORY_PATH, return_value=producer):
        service = JobService(AsyncMock())
        await service._publish_job("retry-job", "resource-1", retry=True)

    args, _ = producer.send.call_args
    assert args[0] == "ckan.ingest.jobs.retry"
