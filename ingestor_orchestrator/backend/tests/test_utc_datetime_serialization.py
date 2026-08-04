# Pedalin
# Copyright (C) 2025  Pedalin
#
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
"""Test that ORM DateTime columns return timezone-aware datetimes (UTC)."""

from datetime import datetime, timezone

import pytest

from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanDataJobResult,
    CkanInstance,
    LatestResourceJob,
    MetadataSync,
    ResourceMetadataLabel,
)

pytestmark = pytest.mark.asyncio


class TestDateTimeColumnsReturnAwareDatetimes:
    """After ORM roundtrip, every DateTime column must carry tzinfo=UTC."""

    @staticmethod
    def _assert_aware(value: datetime, label: str) -> None:
        assert value.tzinfo is not None, (
            f"{label} is naive — expected timezone-aware (UTC)"
        )
        assert value.utcoffset().total_seconds() == 0, (
            f"{label} offset is {value.utcoffset()}, expected 0 (UTC)"
        )

    @staticmethod
    def _assert_aware_or_none(value: datetime | None, label: str) -> None:
        if value is not None:
            assert value.tzinfo is not None, (
                f"{label} is naive — expected timezone-aware (UTC)"
            )
            assert value.utcoffset().total_seconds() == 0, (
                f"{label} offset is {value.utcoffset()}, expected 0 (UTC)"
            )

    # ── CkanDataJob ──────────────────────────────────────────────

    async def test_job_created_at_is_aware(self, db_session, default_instance):
        now = datetime.now(timezone.utc)
        job = CkanDataJob(
            resource_id="r-1",
            dataset_name="ds",
            idempotency_key="k-1",
            instance_id=default_instance.id,
            created_at=now,
        )
        db_session.add(job)
        await db_session.flush()
        await db_session.refresh(job)

        self._assert_aware(job.created_at, "CkanDataJob.created_at")
        self._assert_aware(job.updated_at, "CkanDataJob.updated_at")

    async def test_job_nullable_dates_are_aware(self, db_session, default_instance):
        now = datetime.now(timezone.utc)
        job = CkanDataJob(
            resource_id="r-2",
            dataset_name="ds",
            idempotency_key="k-2",
            instance_id=default_instance.id,
            created_at=now,
            started_at=now,
            completed_at=now,
        )
        db_session.add(job)
        await db_session.flush()
        await db_session.refresh(job)

        self._assert_aware(job.started_at, "CkanDataJob.started_at")
        self._assert_aware(job.completed_at, "CkanDataJob.completed_at")

    # ── CkanDataJobResult ────────────────────────────────────────

    async def test_job_result_created_at_is_aware(self, db_session, default_instance):
        job = CkanDataJob(
            resource_id="r-3",
            dataset_name="ds",
            idempotency_key="k-3",
            instance_id=default_instance.id,
        )
        db_session.add(job)
        await db_session.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            created_at=datetime.now(timezone.utc),
        )
        db_session.add(result)
        await db_session.flush()
        await db_session.refresh(result)

        self._assert_aware(result.created_at, "CkanDataJobResult.created_at")

    # ── CkanInstance ─────────────────────────────────────────────

    async def test_instance_dates_are_aware(self, db_session, default_instance):
        await db_session.refresh(default_instance)

        self._assert_aware(default_instance.created_at, "CkanInstance.created_at")
        self._assert_aware(default_instance.updated_at, "CkanInstance.updated_at")

    async def test_instance_last_metadata_synced_is_aware_or_none(self, db_session, default_instance):
        default_instance.last_metadata_synced = datetime.now(timezone.utc)
        await db_session.flush()
        await db_session.refresh(default_instance)

        self._assert_aware(
            default_instance.last_metadata_synced,
            "CkanInstance.last_metadata_synced",
        )

    # ── LatestResourceJob ────────────────────────────────────────

    async def test_latest_resource_job_dates_are_aware(self, db_session, default_instance):
        job = CkanDataJob(
            resource_id="r-4",
            dataset_name="ds",
            idempotency_key="k-4",
            instance_id=default_instance.id,
        )
        db_session.add(job)
        await db_session.flush()

        lr = LatestResourceJob(
            resource_id="r-4",
            latest_job_id=job.id,
            instance_id=default_instance.id,
            resource_name="test",
            dataset_name="ds",
            status="completed",
            created_at=datetime.now(timezone.utc),
        )
        db_session.add(lr)
        await db_session.flush()
        await db_session.refresh(lr)

        self._assert_aware(lr.created_at, "LatestResourceJob.created_at")
        self._assert_aware(lr.updated_at, "LatestResourceJob.updated_at")

    # ── MetadataSync ─────────────────────────────────────────────

    async def test_metadata_sync_dates_are_aware(self, db_session, default_instance):
        now = datetime.now(timezone.utc)
        sync = MetadataSync(
            instance_id=default_instance.id,
            start_time=now,
        )
        db_session.add(sync)
        await db_session.flush()
        await db_session.refresh(sync)

        self._assert_aware(sync.start_time, "MetadataSync.start_time")
        # end_time is None by default
        assert sync.end_time is None

    async def test_metadata_sync_end_time_is_aware_when_set(self, db_session, default_instance):
        now = datetime.now(timezone.utc)
        sync = MetadataSync(
            instance_id=default_instance.id,
            start_time=now,
            end_time=now,
        )
        db_session.add(sync)
        await db_session.flush()
        await db_session.refresh(sync)

        self._assert_aware(sync.end_time, "MetadataSync.end_time")

    # ── ResourceMetadataLabel ────────────────────────────────────

    async def test_resource_metadata_label_created_at_is_aware(self, db_session):
        label = ResourceMetadataLabel(
            resource_id="r-5",
            label="test-label",
            created_at=datetime.now(timezone.utc),
        )
        db_session.add(label)
        await db_session.flush()
        await db_session.refresh(label)

        self._assert_aware(label.created_at, "ResourceMetadataLabel.created_at")
