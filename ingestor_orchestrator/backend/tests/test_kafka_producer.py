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
"""Test Kafka producer via confluent-kafka API — using autospec to catch API breaks."""

import json
from unittest.mock import AsyncMock, create_autospec, patch

import pytest
from confluent_kafka import Message, Producer
from ingestor_orchestrator.services.job_service import JobService

_FACTORY_PATH = "ingestor_orchestrator.kafka.get_kafka_producer"


def _autospec_producer(topic="ckan.ingest.jobs", partition=2, offset=42):
    mock = create_autospec(Producer, instance=True)

    def _capture(*args, **kw):
        # confluent-kafka produce(topic, value=None, key=None, on_delivery=None, ...)
        callback = kw.get("on_delivery")
        msg = create_autospec(Message, instance=True)
        msg.topic.return_value = topic
        msg.partition.return_value = partition
        msg.offset.return_value = offset
        if callback:
            callback(None, msg)

    mock.produce.side_effect = _capture
    return mock


@pytest.mark.asyncio
async def test_producer_uses_produce_not_send():
    """autospec ensures only real Producer methods exist — no .send()."""
    producer = _autospec_producer()
    with patch(_FACTORY_PATH, return_value=producer):
        service = JobService(AsyncMock())
        await service._publish_job("test-job", "https://dados.pbh.gov.br")
    producer.produce.assert_called_once()


@pytest.mark.asyncio
async def test_returns_metadata_from_callback():
    producer = _autospec_producer(topic="ckan.ingest.jobs", partition=3, offset=99)
    with patch(_FACTORY_PATH, return_value=producer):
        service = JobService(AsyncMock())
        meta = await service._publish_job("job-x")
    assert meta.topic == "ckan.ingest.jobs"
    assert meta.partition == 3
    assert meta.offset == 99


@pytest.mark.asyncio
async def test_publishes_correct_payload():
    producer = _autospec_producer()
    with patch(_FACTORY_PATH, return_value=producer):
        service = JobService(AsyncMock())
        await service._publish_job("test-job", "https://dados.pbh.gov.br", key="abc")
    args, kwargs = producer.produce.call_args
    # produce(topic, value, key=..., on_delivery=...) — value is 2nd positional arg
    payload_bytes = args[1] if len(args) > 1 else kwargs["value"]
    payload = json.loads(payload_bytes.decode())
    assert payload["job_id"] == "test-job"
    assert kwargs["key"] == b"abc"


@pytest.mark.asyncio
async def test_retry_uses_retry_topic():
    producer = _autospec_producer()
    with patch(_FACTORY_PATH, return_value=producer):
        service = JobService(AsyncMock())
        await service._publish_job("retry-job", retry=True)
    args, _ = producer.produce.call_args
    # produce(topic, value, ...) — topic is 1st positional arg
    assert args[0] == "ckan.ingest.jobs.retry"
