# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

"""Tests for Iggy result consumption and manual offset commits."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestor_orchestrator.result_consumer import ResultConsumer


def message():
    result = MagicMock()
    result.offset.return_value = 42
    result.partition_id.return_value = 3
    return result


@pytest.mark.asyncio
async def test_result_is_processed_by_the_commit_after_callback():
    consumer = AsyncMock()
    received = message()

    async def consume(callback, _shutdown):
        await callback(received)
        await result_consumer.stop()

    consumer.consume_messages.side_effect = consume
    bus = AsyncMock()
    bus.result_consumer.return_value = consumer
    result_consumer = ResultConsumer()
    metadata_consumer = AsyncMock()

    async def consume_metadata(_callback, shutdown):
        await shutdown.wait()

    metadata_consumer.consume_messages.side_effect = consume_metadata
    bus.metadata_sync_result_consumer.return_value = metadata_consumer
    result_consumer._process = AsyncMock()

    with patch("ingestor_orchestrator.result_consumer.get_iggy_bus", return_value=bus):
        await result_consumer.start()

    result_consumer._process.assert_awaited_once_with(received)
    consumer.store_offset.assert_not_awaited()


@pytest.mark.asyncio
async def test_result_offset_is_retained_when_processing_fails():
    consumer = AsyncMock()
    received = message()

    async def consume(callback, _shutdown):
        await result_consumer.stop()
        await callback(received)

    consumer.consume_messages.side_effect = consume
    bus = AsyncMock()
    bus.result_consumer.return_value = consumer
    result_consumer = ResultConsumer()
    metadata_consumer = AsyncMock()

    async def consume_metadata(_callback, shutdown):
        await shutdown.wait()

    metadata_consumer.consume_messages.side_effect = consume_metadata
    bus.metadata_sync_result_consumer.return_value = metadata_consumer
    result_consumer._process = AsyncMock(side_effect=RuntimeError("database down"))

    with patch(
        "ingestor_orchestrator.result_consumer.get_iggy_bus",
        return_value=bus,
    ):
        await result_consumer.start()

    consumer.store_offset.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconnects_after_a_consumer_session_fails():
    first_result_consumer = AsyncMock()
    first_metadata_consumer = AsyncMock()
    second_result_consumer = AsyncMock()
    second_metadata_consumer = AsyncMock()
    second_session_started = asyncio.Event()

    async def fail(_callback, _shutdown):
        raise ConnectionError("Iggy disconnected")

    async def wait_for_first_shutdown(_callback, shutdown):
        await shutdown.wait()

    async def wait_for_second_shutdown(_callback, shutdown):
        second_session_started.set()
        await shutdown.wait()

    first_result_consumer.consume_messages.side_effect = fail
    first_metadata_consumer.consume_messages.side_effect = wait_for_first_shutdown
    second_result_consumer.consume_messages.side_effect = wait_for_second_shutdown
    second_metadata_consumer.consume_messages.side_effect = wait_for_second_shutdown
    bus = AsyncMock()
    bus.result_consumer.side_effect = [
        first_result_consumer,
        second_result_consumer,
    ]
    bus.metadata_sync_result_consumer.side_effect = [
        first_metadata_consumer,
        second_metadata_consumer,
    ]

    result_consumer = ResultConsumer()
    result_consumer.INITIAL_RETRY_DELAY_SECONDS = 0
    with patch("ingestor_orchestrator.result_consumer.get_iggy_bus", return_value=bus):
        task = asyncio.create_task(result_consumer.start())
        await asyncio.wait_for(second_session_started.wait(), timeout=1)

        assert result_consumer.is_connected
        bus.invalidate.assert_awaited_once()

        await result_consumer.stop()
        await asyncio.wait_for(task, timeout=1)

    assert not result_consumer.is_connected
    assert bus.result_consumer.await_count == 2


@pytest.mark.asyncio
async def test_shutdown_during_backoff_does_not_start_another_session():
    result_consumer = ResultConsumer()
    result_consumer.INITIAL_RETRY_DELAY_SECONDS = 60
    bus = AsyncMock()
    bus.result_consumer.side_effect = ConnectionError("Iggy disconnected")

    with patch("ingestor_orchestrator.result_consumer.get_iggy_bus", return_value=bus):
        task = asyncio.create_task(result_consumer.start())
        for _ in range(100):
            if bus.invalidate.await_count:
                break
            await asyncio.sleep(0)

        assert bus.invalidate.await_count == 1
        await result_consumer.stop()
        await asyncio.wait_for(task, timeout=1)

    assert bus.result_consumer.await_count == 1
