# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Tests for the Apache Iggy message bus adapter."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestor_orchestrator.iggy_queue import IggyMessageBus


@pytest.fixture
def iggy_settings():
    settings = MagicMock()
    settings.iggy_connection_string = "iggy+tcp://iggy:secret@iggy:8090"
    settings.iggy_stream = "ckan-ingestor"
    settings.iggy_topic = "jobs"
    settings.iggy_topic_retry = "jobs-retry"
    settings.iggy_topic_results = "job-results"
    settings.iggy_metadata_sync_topic = "ckan_metadata_sync"
    settings.iggy_metadata_sync_result_topic = "ckan_metadata_sync_result"
    settings.iggy_metadata_sync_result_group_id = "metadata-results"
    settings.iggy_partitions = 10
    return settings


@pytest.mark.asyncio
async def test_connect_creates_stream_and_topics_idempotently(iggy_settings):
    client = MagicMock()
    client.connect = AsyncMock()
    client.get_stream = AsyncMock(return_value=None)
    client.create_stream = AsyncMock()
    client.get_topic = AsyncMock(return_value=None)
    client.create_topic = AsyncMock()

    with patch(
        "ingestor_orchestrator.iggy_queue.IggyClient.from_connection_string",
        return_value=client,
    ):
        bus = IggyMessageBus(iggy_settings)
        await bus.connect()

    client.create_stream.assert_awaited_once_with(name="ckan-ingestor")
    assert client.create_topic.await_count == 5
    calls_by_topic = {
        call.kwargs["name"]: call for call in client.create_topic.await_args_list
    }
    for topic, call in calls_by_topic.items():
        assert call.kwargs["stream"] == "ckan-ingestor"
        expected = 1 if topic.startswith("ckan_metadata_sync") else 10
        assert call.kwargs["partitions_count"] == expected


@pytest.mark.asyncio
async def test_metadata_sync_messages_always_use_partition_zero(iggy_settings):
    client = MagicMock()
    client.send_messages = AsyncMock()
    bus = IggyMessageBus(iggy_settings, client=client)

    metadata = await bus.publish(
        "ckan_metadata_sync", b'{}', key="sync-with-any-hash"
    )

    assert metadata.partition == 0
    assert client.send_messages.await_args.kwargs["partitioning"] == 0


@pytest.mark.asyncio
async def test_connect_accepts_topology_created_concurrently(iggy_settings):
    client = MagicMock()
    client.connect = AsyncMock()
    client.get_stream = AsyncMock(side_effect=[None, object()])
    client.create_stream = AsyncMock(side_effect=RuntimeError("already exists"))
    client.get_topic = AsyncMock(side_effect=[None, object(), object(), object(), object(), object()])
    client.create_topic = AsyncMock(side_effect=RuntimeError("already exists"))

    with patch(
        "ingestor_orchestrator.iggy_queue.IggyClient.from_connection_string",
        return_value=client,
    ):
        bus = IggyMessageBus(iggy_settings)
        await bus.connect()

    assert client.get_stream.await_count == 2
    assert client.get_topic.await_count == 6


@pytest.mark.asyncio
async def test_publish_routes_deterministically_and_returns_generic_metadata(
    iggy_settings,
):
    client = MagicMock()
    client.send_messages = AsyncMock()
    bus = IggyMessageBus(iggy_settings, client=client)

    metadata = await bus.publish("jobs", b'{"job_id":"job-1"}', key="job-1")

    assert metadata.broker_type == "iggy"
    assert metadata.stream == "ckan-ingestor"
    assert metadata.topic == "jobs"
    assert 0 <= metadata.partition < 10
    assert metadata.offset is None
    client.send_messages.assert_awaited_once()
    assert client.send_messages.await_args.kwargs["partitioning"] == metadata.partition


@pytest.mark.asyncio
async def test_publish_uses_same_partition_for_same_key(iggy_settings):
    client = MagicMock()
    client.send_messages = AsyncMock()
    bus = IggyMessageBus(iggy_settings, client=client)

    first = await bus.publish("jobs", b"first", key="stable-key")
    second = await bus.publish("jobs", b"second", key="stable-key")

    assert first.partition == second.partition
