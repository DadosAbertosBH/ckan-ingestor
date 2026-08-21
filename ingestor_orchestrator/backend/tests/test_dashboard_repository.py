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
"""Tests for DashboardRepository — verifies stats aggregation."""

from datetime import datetime, timezone

import pytest

from ingestor_orchestrator.dto import InstanceStats
from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanInstance,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.repositories.sqlalchemy_dashboard_repository import (
    SqlAlchemyDashboardRepository,
)

pytestmark = pytest.mark.asyncio


async def _create_job(
    sess,
    instance: CkanInstance,
    resource_id: str,
    status: JobStatus,
    *,
    is_latest: bool = True,
    latest_status: JobStatus | None = None,
):
    """Helper to create a CkanDataJob with a specific status."""
    job = CkanDataJob(
        resource_id=resource_id,
        resource_name="Test Resource",
        resource_url=None,
        resource_format="CSV",
        dataset_name="ds-test",
        status=status,
        idempotency_key=resource_id,
        instance_id=instance.id,
        ckan_url=instance.url,
    )
    sess.add(job)
    await sess.flush()

    if is_latest:
        sess.add(
            LatestResourceJob(
                resource_id=resource_id,
                instance_id=instance.id,
                latest_job_id=job.id,
                resource_name=job.resource_name,
                resource_url=job.resource_url,
                resource_format=job.resource_format,
                dataset_name=job.dataset_name,
                status=latest_status or job.status,
            )
        )
        await sess.flush()

    return job


async def _create_empty_label(sess, instance: CkanInstance, resource_id: str):
    """Helper: create a completed latest job + ResourceMetadataLabel("empty")."""
    await _create_job(sess, instance, resource_id, JobStatus.COMPLETED)

    label = ResourceMetadataLabel(resource_id=resource_id, label="empty")
    sess.add(label)
    await sess.flush()


class TestDashboardRepository:
    async def test_get_stats_no_instances(self, db_session):
        """get_stats returns empty list when no instances exist."""
        repo = SqlAlchemyDashboardRepository(db_session)
        result = await repo.get_stats()
        assert result == []

    async def test_get_stats_with_job_counts(self, db_session, default_instance):
        """get_stats returns correct job counts per instance."""
        await _create_job(db_session, default_instance, "r-pending", JobStatus.PENDING)
        await _create_job(db_session, default_instance, "r-completed-1", JobStatus.COMPLETED)
        await _create_job(db_session, default_instance, "r-completed-2", JobStatus.COMPLETED)
        await _create_job(db_session, default_instance, "r-failed", JobStatus.FAILED)
        await _create_job(db_session, default_instance, "r-processing", JobStatus.PROCESSING)

        repo = SqlAlchemyDashboardRepository(db_session)
        result = await repo.get_stats()

        assert len(result) == 1
        stats = result[0]
        assert isinstance(stats, InstanceStats)
        assert stats.instance.id == default_instance.id
        assert stats.pending == 1
        assert stats.processing == 1
        assert stats.completed == 2
        assert stats.failed == 1
        assert stats.empty == 0

    async def test_get_stats_counts_only_latest_job_per_resource(
        self, db_session, default_instance
    ):
        """Historical jobs do not contribute to dashboard status totals."""
        await _create_job(
            db_session,
            default_instance,
            "r-reprocessed",
            JobStatus.FAILED,
            is_latest=False,
        )
        await _create_job(
            db_session,
            default_instance,
            "r-reprocessed",
            JobStatus.COMPLETED,
        )

        repo = SqlAlchemyDashboardRepository(db_session)
        result = await repo.get_stats()

        assert len(result) == 1
        assert result[0].completed == 1
        assert result[0].failed == 0

    async def test_get_stats_uses_the_latest_job_current_status(
        self, db_session, default_instance
    ):
        """Dashboard reads the job status rather than its stale denormalized copy."""
        await _create_job(
            db_session,
            default_instance,
            "r-processing",
            JobStatus.PROCESSING,
            latest_status=JobStatus.PENDING,
        )

        repo = SqlAlchemyDashboardRepository(db_session)
        result = await repo.get_stats()

        assert len(result) == 1
        assert result[0].pending == 0
        assert result[0].processing == 1

    async def test_get_stats_includes_empty_count(self, db_session, default_instance):
        """get_stats includes empty resource count."""
        await _create_job(db_session, default_instance, "r-completed", JobStatus.COMPLETED)
        await _create_empty_label(db_session, default_instance, "r-empty-1")
        await _create_empty_label(db_session, default_instance, "r-empty-2")

        repo = SqlAlchemyDashboardRepository(db_session)
        result = await repo.get_stats()

        assert len(result) == 1
        stats = result[0]
        assert stats.completed == 3
        assert stats.empty == 2

    async def test_get_stats_multiple_instances(self, db_session, default_instance):
        """get_stats returns stats for all instances."""
        other = CkanInstance(
            id="inst-other",
            name="Other",
            url="https://other.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(other)
        await db_session.flush()

        await _create_job(db_session, default_instance, "r-1", JobStatus.COMPLETED)
        await _create_job(db_session, other, "r-2", JobStatus.PENDING)
        await _create_empty_label(db_session, default_instance, "r-empty")

        repo = SqlAlchemyDashboardRepository(db_session)
        result = await repo.get_stats()

        assert len(result) == 2
        # Ordered by instance name (Default < Other)
        assert result[0].instance.name == "Default"
        assert result[1].instance.name == "Other"
        assert result[0].completed == 2
        assert result[0].pending == 0
        assert result[0].empty == 1
        assert result[1].completed == 0
        assert result[1].pending == 1
        assert result[1].empty == 0
