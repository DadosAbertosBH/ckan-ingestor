# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

"""Tests for Iggy result consumption and manual offset commits."""

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

    consumer.consume_messages.side_effect = consume
    bus = AsyncMock()
    bus.result_consumer.return_value = consumer
    result_consumer = ResultConsumer()
    result_consumer._process = AsyncMock()

    with patch(
        "ingestor_orchestrator.result_consumer.get_iggy_bus", return_value=bus
    ):
        await result_consumer.start()

    result_consumer._process.assert_awaited_once_with(received)
    consumer.store_offset.assert_not_awaited()


@pytest.mark.asyncio
async def test_result_offset_is_retained_when_processing_fails():
    consumer = AsyncMock()
    received = message()

    async def consume(callback, _shutdown):
        await callback(received)

    consumer.consume_messages.side_effect = consume
    bus = AsyncMock()
    bus.result_consumer.return_value = consumer
    result_consumer = ResultConsumer()
    result_consumer._process = AsyncMock(side_effect=RuntimeError("database down"))

    with (
        patch(
            "ingestor_orchestrator.result_consumer.get_iggy_bus",
            return_value=bus,
        ),
        pytest.raises(RuntimeError, match="database down"),
    ):
        await result_consumer.start()

    consumer.store_offset.assert_not_awaited()
