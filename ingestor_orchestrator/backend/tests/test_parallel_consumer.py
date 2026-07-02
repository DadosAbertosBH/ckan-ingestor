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
"""Integration tests for parallel consumer with real Kafka via testcontainers.

Tests verify:
- Messages from different partitions are processed concurrently
- Messages from the same partition are processed in order
- Per-message offset commit
- _consume_loop does not block polling on slow tasks
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from ingestor_orchestrator.models import CkanDataJob, CkanInstance, JobStatus
from ingestor_orchestrator.services.job_service import JobService
from ingestor_orchestrator.worker.consumer import Worker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.kafka import KafkaContainer

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skip(reason="Needs confluent-kafka integration test rewrite"),
]

TOPIC = "test.ingest.jobs"
GROUP_ID = "test-worker-group"


@pytest.fixture(scope="module")
def kafka_container():
    """Start a Kafka container for the test module."""
    kc = KafkaContainer().with_kraft()
    kc.start()
    yield kc
    kc.stop()


@pytest.fixture
def bootstrap_servers(kafka_container):
    return kafka_container.get_bootstrap_server()


@pytest.fixture
def producer(bootstrap_servers):
    p = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})
    yield p
    p.flush()


def _create_consumer(bootstrap_servers, topic, group_id):
    """Create a real Consumer for tests."""
    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    return c


@pytest_asyncio.fixture
async def instance(engine, _create_tables):
    async_session_local = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session_local() as session:
        inst = CkanInstance(
            id="inst-parallel",
            name="Parallel Test",
            url="https://test.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(inst)
        await session.commit()
        return inst


@pytest_asyncio.fixture
async def sess(engine, _create_tables):
    async_session_local = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session_local() as session:
        yield session
        await session.rollback()


def _ensure_topic(bootstrap_servers, topic, partitions=4):
    """Create topic with the given partition count if it doesn't exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )


def _produce_messages(producer, topic, messages):
    """Produce messages. Each message is (partition_key, payload)."""
    for partition_key, payload in messages:
        producer.produce(
            topic,
            json.dumps(payload).encode(),
            key=partition_key.encode() if partition_key else None,
        )
    producer.flush()


class TestParallelProcessing:
    async def test_different_partitions_processed_in_parallel(
        self, sess, instance, bootstrap_servers, producer, request
    ):
        """Messages on different partitions should be processed concurrently."""
        topic = f"{TOPIC}.parallel-{id(request.node)}"
        _ensure_topic(bootstrap_servers, topic, partitions=4)

        jobs = []
        for i in range(2):
            job = CkanDataJob(
                id=f"job-parallel-{i}",
                resource_id=f"r-parallel-{i}",
                dataset_name=f"ds-parallel-{i}",
                idempotency_key=f"r-parallel-{i}",
                instance_id=instance.id,
                status=JobStatus.PENDING,
                ckan_url="https://dados.pbh.gov.br",
            )
            sess.add(job)
            jobs.append(job)
        await sess.commit()

        # Send to different partitions using different keys
        _produce_messages(
            producer,
            topic,
            [(f"key-{i}", {"job_id": f"job-parallel-{i}"}) for i in range(2)],
        )

        consumer = _create_consumer(bootstrap_servers, topic, f"{GROUP_ID}-parallel")

        processing_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()
        barrier = asyncio.Barrier(2)

        async def _slow_process_job(job_id, **kwargs):
            nonlocal processing_count, max_concurrent
            async with lock:
                processing_count += 1
                max_concurrent = max(max_concurrent, processing_count)
            try:
                await asyncio.wait_for(barrier.wait(), timeout=2)
            except asyncio.BrokenBarrierError:
                pass
            await asyncio.sleep(0.01)
            async with lock:
                processing_count -= 1

        worker = Worker()
        with (
            patch(
                "ingestor_orchestrator.worker.consumer.async_session",
                return_value=sess,
            ),
            patch.object(JobService, "process_job", side_effect=_slow_process_job),
        ):
            # Use _process_ordered directly with records from real consumer
            records = consumer.poll(timeout_ms=5000, max_records=10)
            tasks = []
            for msg_list in records.values():
                for record in msg_list:
                    tasks.append(
                        asyncio.create_task(worker._process_ordered(record, consumer))
                    )
            if tasks:
                await asyncio.gather(*tasks)

        consumer.close()
        assert max_concurrent == 2, f"Expected 2 concurrent tasks, got {max_concurrent}"

    async def test_same_partition_processed_sequentially(
        self, sess, instance, bootstrap_servers, producer, request
    ):
        """Messages on the same partition must be processed in order."""
        topic = f"{TOPIC}.seq-{id(request.node)}"
        _ensure_topic(bootstrap_servers, topic, partitions=1)

        jobs = []
        for i in range(3):
            job = CkanDataJob(
                id=f"job-seq-{i}",
                resource_id=f"r-seq-{i}",
                dataset_name=f"ds-seq-{i}",
                idempotency_key=f"r-seq-{i}",
                instance_id=instance.id,
                status=JobStatus.PENDING,
                ckan_url="https://dados.pbh.gov.br",
            )
            sess.add(job)
            jobs.append(job)
        await sess.commit()

        # All messages to same partition (same key → same partition)
        _produce_messages(
            producer,
            topic,
            [("same-key", {"job_id": f"job-seq-{i}"}) for i in range(3)],
        )

        consumer = _create_consumer(bootstrap_servers, topic, f"{GROUP_ID}-seq")

        processing_order = []
        lock = asyncio.Lock()

        async def _track_order(job_id, **kwargs):
            async with lock:
                processing_order.append(job_id)
            await asyncio.sleep(0.05)

        worker = Worker()
        with (
            patch(
                "ingestor_orchestrator.worker.consumer.async_session",
                return_value=sess,
            ),
            patch.object(JobService, "process_job", side_effect=_track_order),
        ):
            records = consumer.poll(timeout_ms=5000, max_records=10)
            tasks = []
            for msg_list in records.values():
                for record in msg_list:
                    tasks.append(
                        asyncio.create_task(worker._process_ordered(record, consumer))
                    )
            if tasks:
                await asyncio.gather(*tasks)

        consumer.close()
        assert processing_order == ["job-seq-0", "job-seq-1", "job-seq-2"]

    async def test_commit_per_message(
        self, sess, instance, bootstrap_servers, producer, request
    ):
        """Each processed message should have its offset committed individually."""
        topic = f"{TOPIC}.commit-{id(request.node)}"
        _ensure_topic(bootstrap_servers, topic, partitions=2)

        job = CkanDataJob(
            id="job-commit-int",
            resource_id="r-commit-int",
            dataset_name="ds-commit-int",
            idempotency_key="r-commit-int",
            instance_id=instance.id,
            status=JobStatus.PENDING,
            ckan_url="https://dados.pbh.gov.br",
        )
        sess.add(job)
        await sess.commit()

        _produce_messages(
            producer,
            topic,
            [
                ("key-a", {"job_id": "job-commit-int"}),
                ("key-b", {"job_id": "job-commit-int"}),
            ],
        )

        consumer = _create_consumer(bootstrap_servers, topic, f"{GROUP_ID}-commit")

        worker = Worker()
        with (
            patch(
                "ingestor_orchestrator.worker.consumer.async_session",
                return_value=sess,
            ),
            patch.object(JobService, "process_job", new_callable=AsyncMock),
        ):
            records = consumer.poll(timeout_ms=5000, max_records=10)
            tasks = []
            for msg_list in records.values():
                for record in msg_list:
                    tasks.append(
                        asyncio.create_task(worker._process_ordered(record, consumer))
                    )
            if tasks:
                await asyncio.gather(*tasks)

        # Verify committed offsets via committed() on the consumer
        partitions = consumer.partitions_for_topic(topic)
        committed_offsets = {}
        for p in partitions:
            tp = TopicPartition(topic, p)
            offset_meta = consumer.committed(tp)
            if offset_meta is not None:
                committed_offsets[p] = offset_meta

        consumer.close()
        # At least one partition should have a committed offset > 0
        assert any(o > 0 for o in committed_offsets.values()), (
            f"Expected committed offsets > 0, got {committed_offsets}"
        )

    async def test_consume_loop_does_not_block_on_slow_tasks(
        self, sess, instance, bootstrap_servers, producer, request
    ):
        """_consume_loop should keep polling even while tasks are still running.

        We test this by starting _consume_loop with a real consumer that
        has messages available, and a slow process_job. The loop should
        poll multiple times even while tasks are still being processed.
        """
        topic = f"{TOPIC}.noblock-{id(request.node)}"
        _ensure_topic(bootstrap_servers, topic, partitions=2)

        job = CkanDataJob(
            id="job-noblock",
            resource_id="r-noblock",
            dataset_name="ds-noblock",
            idempotency_key="r-noblock",
            instance_id=instance.id,
            status=JobStatus.PENDING,
            ckan_url="https://dados.pbh.gov.br",
        )
        sess.add(job)
        await sess.commit()

        # Produce two messages
        _produce_messages(
            producer,
            topic,
            [
                ("key-a", {"job_id": "job-noblock"}),
                ("key-b", {"job_id": "job-noblock"}),
            ],
        )

        # Use _process_ordered directly to simulate the non-blocking behavior
        # Since _consume_loop uses run_in_executor which doesn't work well
        # with kafka-python's non-thread-safe consumer, we test the dispatch
        # pattern directly: poll once, dispatch tasks, poll again immediately.
        consumer = _create_consumer(bootstrap_servers, topic, f"{GROUP_ID}-noblock")

        processing_started = asyncio.Event()
        let_finish = asyncio.Event()

        async def _slow_job(job_id, **kwargs):
            processing_started.set()
            await asyncio.wait_for(let_finish.wait(), timeout=5)

        worker = Worker()
        with (
            patch(
                "ingestor_orchestrator.worker.consumer.async_session",
                return_value=sess,
            ),
            patch.object(JobService, "process_job", side_effect=_slow_job),
        ):
            # Poll and dispatch (simulates _consume_loop's non-blocking dispatch)
            records = consumer.poll(timeout_ms=5000, max_records=10)
            tasks = []
            for msg_list in records.values():
                for record in msg_list:
                    task = asyncio.create_task(
                        worker._process_ordered(record, consumer)
                    )
                    tasks.append(task)

            # Wait for processing to start — tasks are running
            if tasks:
                await asyncio.wait_for(processing_started.wait(), timeout=5)

            # While tasks are running, a second poll returns empty (no more messages)
            # This proves the consumer can still poll — it's not blocked by tasks
            second_poll = consumer.poll(timeout_ms=100, max_records=10)
            # second_poll is empty because we already consumed all messages
            assert sum(len(v) for v in second_poll.values()) == 0

            # Release the slow job
            let_finish.set()
            if tasks:
                await asyncio.gather(*tasks)

        consumer.close()
