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
"""Test that generic broker routing metadata is populated on job creation."""

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
            id="inst-broker-md",
            name="Broker Metadata Test",
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


class TestMessageMetadataOnCreate:
    async def test_create_job_stores_iggy_metadata(self, autocommit_session, instance):
        """create_job stores broker, stream, topic and partition metadata."""
        service = JobService(autocommit_session)

        job = await service.create_job(
            JobCreate(
                resource_id="res-1",
                dataset_name="test-dataset",
                instance_id=instance.id,
                ckan_url="https://example.com",
            )
        )

        assert job.broker_type == "iggy"
        assert job.message_stream == "ckan-ingestor"
        assert job.message_topic == "jobs"
        assert job.message_partition == 0
        assert job.message_offset is None

    async def test_retry_job_stores_generic_metadata(
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

        assert retried.broker_type == "iggy"
        assert retried.message_stream == "ckan-ingestor"
        assert retried.message_topic == "jobs-retry"
        assert retried.message_partition is not None

    async def test_new_jobs_have_metadata_for_debug(self, autocommit_session, instance):
        """All newly created jobs have generic broker metadata for debugging."""
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

        assert job.broker_type == "iggy"
        assert job.message_stream is not None
        assert job.message_topic is not None
        assert job.message_partition is not None
