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
"""Test Kafka consumer commits per-record offsets, not global."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from confluent_kafka import TopicPartition
from ingestor_orchestrator.models import CkanDataJob, CkanInstance, JobStatus
from ingestor_orchestrator.services.job_service import JobService
from ingestor_orchestrator.worker.consumer import Worker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skip(reason="Needs confluent-kafka rewrite"),
]


@pytest_asyncio.fixture
async def instance(engine, _create_tables):
    async_session_local = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session_local() as session:
        inst = CkanInstance(
            id="inst-kafka-commit",
            name="Kafka Commit Test",
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


class TestKafkaCommitPerRecord:
    async def test_process_commits_after_each_record(self, sess, instance):
        """Sequential processing: commit() is called after each record."""
        job = CkanDataJob(
            id="job-kafka-2",
            resource_id="r-kafka",
            dataset_name="ds-kafka",
            idempotency_key="r-kafka",
            instance_id=instance.id,
            status=JobStatus.PENDING,
            ckan_url="https://dados.pbh.gov.br",
        )
        sess.add(job)
        await sess.commit()

        mock_consumer = MagicMock()
        mock_consumer.commit = MagicMock()

        record = MagicMock()
        record.topic.return_value = "ckan.ingest.jobs"
        record.partition.return_value = 0
        record.offset.return_value = 42
        record.value.return_value = json.dumps({"job_id": "job-kafka-2"}).encode()

        worker = Worker()
        with (
            patch(
                "ingestor_orchestrator.worker.consumer.async_session", return_value=sess
            ),
            patch.object(JobService, "process_job", new_callable=AsyncMock),
        ):
            await worker._process(record, mock_consumer)

        # Per-offset commit: called with TopicPartition + OffsetAndMetadata
        mock_consumer.commit.assert_called_once()
        args, _ = mock_consumer.commit.call_args
        committed = args[0]
        assert isinstance(committed, dict)
        tp = TopicPartition("ckan.ingest.jobs", 0)
        assert tp in committed
        assert committed[tp].offset == 43  # offset + 1
