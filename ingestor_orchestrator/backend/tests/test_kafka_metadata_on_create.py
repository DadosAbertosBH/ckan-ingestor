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
"""Test that kafka_topic/partition/offset are populated on job creation."""

from datetime import datetime, timezone

import pytest
import pytest_asyncio
from ingestor_orchestrator.dto import JobCreate
from ingestor_orchestrator.models import CkanInstance, JobStatus
from ingestor_orchestrator.services.job_service import JobService
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def instance(engine, _create_tables):
    """Create a CkanInstance for tests."""
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        inst = CkanInstance(
            id="inst-kafka-md",
            name="Kafka Metadata Test",
            url="https://test.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(inst)
        await session.commit()
        return inst


@pytest_asyncio.fixture
async def autocommit_session(engine, _create_tables):
    """Session without outer transaction — allows internal commits."""
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()


class TestKafkaMetadataOnCreate:
    async def test_create_job_stores_kafka_metadata(
        self, autocommit_session, instance
    ):
        """create_job must populate kafka_topic, partition, and offset."""
        service = JobService(autocommit_session)

        job = await service.create_job(
            JobCreate(
                resource_id="res-1",
                dataset_name="test-dataset",
                instance_id=instance.id,
                ckan_url="https://example.com",
            )
        )

        assert job.kafka_topic == "ckan.ingest.jobs"
        assert job.kafka_partition == 0
        assert job.kafka_offset == 0

    async def test_retry_job_stores_kafka_metadata(
        self, autocommit_session, instance
    ):
        """retry_job republishes to retry topic and updates metadata."""
        service = JobService(autocommit_session)

        # Create and fail a job
        job = await service.create_job(
            JobCreate(
                resource_id="res-retry",
                dataset_name="test-retry",
                instance_id=instance.id,
            )
        )
        job.status = JobStatus.FAILED
        await autocommit_session.flush()

        retried = await service.retry_job(job.id)

        assert retried.kafka_topic is not None, "kafka_topic must be set on retry"
        assert retried.kafka_partition is not None, "kafka_partition must be set on retry"
        assert retried.kafka_offset is not None, "kafka_offset must be set on retry"

    async def test_new_jobs_have_metadata_for_debug(
        self, autocommit_session, instance
    ):
        """All newly created jobs must have kafka metadata for debugging."""
        service = JobService(autocommit_session)

        job = await service.create_job(
            JobCreate(
                resource_id="res-debug",
                dataset_name="debug-dataset",
                instance_id=instance.id,
                resource_name="Test Resource",
                resource_url="https://data.example.com/test.csv",
                resource_format="CSV",
            )
        )

        await autocommit_session.refresh(job)

        assert job.kafka_topic is not None, (
            "kafka_topic must be set on job creation for debugging"
        )
        assert job.kafka_partition is not None, (
            "kafka_partition must be set on job creation for debugging"
        )
        assert job.kafka_offset is not None, (
            "kafka_offset must be set on job creation for debugging"
        )
