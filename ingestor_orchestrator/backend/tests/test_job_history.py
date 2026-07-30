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
"""Tests for job history — multiple jobs per resource."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from ingestor_orchestrator.models import CkanDataJob, CkanInstance, JobStatus
from ingestor_orchestrator.schemas import JobCreate
from ingestor_orchestrator.services.job_service import JobService
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def instance(engine, _create_tables):
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        inst = CkanInstance(
            id="inst-history",
            name="History Test",
            url="https://test.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(inst)
        await session.commit()
        return inst


@pytest_asyncio.fixture
async def sess(engine, _create_tables):
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()


class TestJobHistory:
    async def test_multiple_jobs_same_resource(self, sess, instance):
        """Can create multiple jobs for the same resource (history)."""
        service = JobService(sess)
        jc = JobCreate(resource_id="r1", dataset_name="d1", instance_id=instance.id)
        job1 = await service.create_job(jc)

        job1.status = JobStatus.COMPLETED
        await sess.commit()

        job2 = await service.create_job(jc)
        assert job1.id != job2.id
        assert job2.status == JobStatus.PENDING

    async def test_creates_new_job_when_pending_exists(self, sess, instance):
        """Always creates new job — caller handles deduplication."""
        service = JobService(sess)
        jc = JobCreate(resource_id="r2", dataset_name="d2", instance_id=instance.id)

        job1 = await service.create_job(jc)
        job2 = await service.create_job(jc)

        assert job1.id != job2.id
        assert job1.status == JobStatus.PENDING

    async def test_creates_when_processing_exists(self, sess, instance):
        """Creates new job even when PROCESSING exists."""
        job = CkanDataJob(
            resource_id="r3",
            dataset_name="d3",
            idempotency_key="r3",
            instance_id=instance.id,
            status=JobStatus.PROCESSING,
        )
        sess.add(job)
        await sess.commit()

        service = JobService(sess)
        jc = JobCreate(resource_id="r3", dataset_name="d3", instance_id=instance.id)
        new_job = await service.create_job(jc)
        assert new_job.id != job.id
        assert new_job.status == JobStatus.PENDING

    async def test_creates_new_job_after_completed(self, sess, instance):
        """Creates new job after previous one completed (preserves history)."""
        job = CkanDataJob(
            resource_id="r4",
            dataset_name="d4",
            idempotency_key="r4",
            instance_id=instance.id,
            status=JobStatus.COMPLETED,
        )
        sess.add(job)
        await sess.commit()

        service = JobService(sess)
        jc = JobCreate(resource_id="r4", dataset_name="d4", instance_id=instance.id)
        new_job = await service.create_job(jc)
        assert new_job.id != job.id
        assert new_job.status == JobStatus.PENDING

        count = (
            await sess.execute(
                select(func.count()).where(CkanDataJob.resource_id == "r4")
            )
        ).scalar()
        assert count == 2

    async def test_creates_new_job_after_failed(self, sess, instance):
        """Creates new job after previous one failed (preserves history)."""
        job = CkanDataJob(
            resource_id="r5",
            dataset_name="d5",
            idempotency_key="r5",
            instance_id=instance.id,
            status=JobStatus.FAILED,
        )
        sess.add(job)
        await sess.commit()

        service = JobService(sess)
        jc = JobCreate(resource_id="r5", dataset_name="d5", instance_id=instance.id)
        new_job = await service.create_job(jc)
        assert new_job.id != job.id
        assert new_job.status == JobStatus.PENDING

    async def test_retry_publishes_to_retry_subject(self, sess, instance):
        """retry_job must publish to the retry subject for priority."""
        job = CkanDataJob(
            resource_id="r-retry",
            dataset_name="d-retry",
            idempotency_key="r-retry",
            instance_id=instance.id,
            status=JobStatus.FAILED,
        )
        sess.add(job)
        await sess.commit()

        service = JobService(sess)
        mock_publish = AsyncMock()
        with patch.object(service, "_publish_job", mock_publish):
            await service.retry_job(job.id)

            mock_publish.assert_awaited_once()
            assert mock_publish.call_args.kwargs.get("retry") is True, (
                "retry_job must call _publish_job with retry=True"
            )

    async def test_job_results_ordered_by_created_at(self, sess, instance):
        """Job results must be ordered by created_at ascending."""
        from datetime import datetime, timedelta, timezone

        from ingestor_orchestrator.models.ckan_data_job_result import CkanDataJobResult

        job = CkanDataJob(
            resource_id="r-order",
            dataset_name="d-order",
            idempotency_key="r-order",
            instance_id=instance.id,
            status=JobStatus.FAILED,
        )
        sess.add(job)
        await sess.flush()

        # Add results in reverse chronological order
        t1 = datetime.now(timezone.utc)
        t2 = t1 + timedelta(seconds=1)
        t3 = t1 + timedelta(seconds=2)

        sess.add(
            CkanDataJobResult(
                job_id=job.id,
                success=False,
                created_at=t3,
                error_message="third",
            )
        )
        sess.add(
            CkanDataJobResult(
                job_id=job.id,
                success=False,
                created_at=t1,
                error_message="first",
            )
        )
        sess.add(
            CkanDataJobResult(
                job_id=job.id,
                success=False,
                created_at=t2,
                error_message="second",
            )
        )
        await sess.commit()

        # Reload with relationship
        result = await sess.get(CkanDataJob, job.id)
        await sess.refresh(result, attribute_names=["results"])

        # Must be ordered by created_at ascending
        assert len(result.results) == 3
        assert result.results[0].error_message == "first"
        assert result.results[1].error_message == "second"
        assert result.results[2].error_message == "third"
