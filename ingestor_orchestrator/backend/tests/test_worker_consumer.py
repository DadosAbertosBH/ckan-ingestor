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
"""Test worker consumer loop: sequential within a loop, parallel across loops.

kafka-python consumer: uses poll(timeout_ms, max_records) returning
{TopicPartition: [ConsumerRecord, ...]} for batch processing.
Per-partition locks ensure ordering; semaphore caps concurrency.
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from kafka import TopicPartition
from kafka.consumer.fetcher import ConsumerRecord


def _make_record(job_id, topic="ckan.ingest.jobs", partition=0, offset=0):
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=1234567890,
        timestamp_type=0,
        key=None,
        value=json.dumps({"job_id": job_id, "ckan_url": ""}).encode(),
        headers=[],
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
        serialized_header_size=-1,
        leader_epoch=0,
    )


def _make_consumer(messages):
    """Fake KafkaConsumer that returns batches via poll(), then empty dicts.

    kafka-python poll() returns dict[TopicPartition, list[ConsumerRecord]].
    """
    batches = [
        {TopicPartition(m.topic, m.partition): [m]}
        for m in messages
    ]
    idx = [0]

    def poll(timeout_ms=None, max_records=None):
        if idx[0] < len(batches):
            batch = batches[idx[0]]
            idx[0] += 1
            return batch
        return {}

    return MagicMock(poll=poll)


@pytest.mark.asyncio
async def test_single_consumer_sequential():
    """Messages from one consumer are processed in order."""
    from ingestor_orchestrator.worker.consumer import Worker

    order = []

    worker = Worker()
    worker._running = True
    processed = [0]

    async def tracked_process(record, consumer):
        order.append(json.loads(record.value.decode())["job_id"])
        await asyncio.sleep(0.01)
        processed[0] += 1
        if processed[0] >= len(msgs):
            worker._running = False

    worker._process = tracked_process

    msgs = [_make_record("a"), _make_record("b"), _make_record("c")]
    consumer = _make_consumer(msgs)

    with (
        patch("ingestor_orchestrator.worker.consumer.async_session"),
        patch("ingestor_orchestrator.worker.consumer.JobService"),
    ):
        await asyncio.wait_for(worker._consume_loop(consumer, 100), timeout=5)

    assert order == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_two_consumers_run_in_parallel():
    """Two consume_loops (main + retry) process concurrently — up to 2 at a time."""
    from ingestor_orchestrator.worker.consumer import Worker

    active = 0
    max_active = 0
    lock = asyncio.Lock()

    worker = Worker()
    worker._running = True
    processed_count = [0]

    async def tracked_process(record, consumer):
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(0.05)
        async with lock:
            active -= 1
        processed_count[0] += 1
        if processed_count[0] >= 4:
            worker._running = False

    worker._process = tracked_process

    consumer1 = _make_consumer([_make_record("1a", partition=0), _make_record("1b", partition=0)])
    consumer2 = _make_consumer([_make_record("2a", partition=1), _make_record("2b", partition=1)])

    with (
        patch("ingestor_orchestrator.worker.consumer.async_session"),
        patch("ingestor_orchestrator.worker.consumer.JobService"),
    ):
        await asyncio.wait_for(
            asyncio.gather(
                worker._consume_loop(consumer1, 100),
                worker._consume_loop(consumer2, 100),
            ),
            timeout=5,
        )

    # Two consumer loops = up to 2 concurrent jobs
    assert max_active == 2
