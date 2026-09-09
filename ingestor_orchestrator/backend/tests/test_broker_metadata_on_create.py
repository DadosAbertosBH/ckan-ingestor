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
from ingestor_orchestrator.models import CkanDataJob, CkanInstance, JobStatus
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


class TestRetryBrokerMetadata:
    async def test_retry_job_stores_generic_metadata(
        self, autocommit_session, instance
    ):
        """retry_job republishes to retry topic and updates metadata."""
        service = JobService(autocommit_session)

        job = CkanDataJob(
            resource_id="res-retry",
            dataset_name="test-retry",
            idempotency_key="res-retry",
            instance_id=instance.id,
            status=JobStatus.FAILED,
        )
        autocommit_session.add(job)
        await autocommit_session.commit()

        retried = await service.retry_job(job.id)

        assert retried.broker_type == "iggy"
        assert retried.message_stream == "ckan-ingestor"
        assert retried.message_topic == "jobs-retry"
        assert retried.message_partition is not None
