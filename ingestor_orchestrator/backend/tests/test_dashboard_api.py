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
"""Tests for dashboard stats query with empty resource count."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select

from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanInstance,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)

pytestmark = pytest.mark.asyncio


async def _query_dashboard_stats(db_session):
    """Run the dashboard stats query (mirrors the API endpoint logic)."""
    instance_result = await db_session.execute(
        select(CkanInstance).order_by(CkanInstance.name)
    )
    instances = instance_result.scalars().all()

    # Fetch job counts grouped by instance_id and status
    job_counts = await db_session.execute(
        select(
            CkanDataJob.instance_id,
            CkanDataJob.status,
            func.count(CkanDataJob.id),
        ).group_by(CkanDataJob.instance_id, CkanDataJob.status)
    )
    counts_by_instance: dict[str, dict[JobStatus, int]] = {}
    for row in job_counts:
        inst_id, status, count = row
        counts_by_instance.setdefault(inst_id, {})[JobStatus(status)] = count

    # Fetch empty resource counts grouped by instance_id
    empty_counts_result = await db_session.execute(
        select(
            LatestResourceJob.instance_id,
            func.count().label("empty_count"),
        )
        .join(
            ResourceMetadataLabel,
            (LatestResourceJob.resource_id == ResourceMetadataLabel.resource_id)
            & (ResourceMetadataLabel.label == "empty"),
        )
        .group_by(LatestResourceJob.instance_id)
    )
    empty_counts: dict[str, int] = {
        row.instance_id: row.empty_count for row in empty_counts_result
    }

    result = []
    for inst in instances:
        counts = counts_by_instance.get(inst.id, {})
        result.append(
            {
                "instance_id": inst.id,
                "instance_name": inst.name,
                "pending": counts.get(JobStatus.PENDING, 0),
                "processing": counts.get(JobStatus.PROCESSING, 0),
                "completed": counts.get(JobStatus.COMPLETED, 0),
                "failed": counts.get(JobStatus.FAILED, 0),
                "empty": empty_counts.get(inst.id, 0),
            }
        )

    return result


class TestDashboardStats:
    async def test_empty_count_zero_when_no_labels(self, db_session):
        """Dashboard stats return empty=0 when no resources have the empty label."""
        inst = CkanInstance(
            id="inst-1",
            name="Instance 1",
            url="https://example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(inst)
        await db_session.flush()

        now = datetime.now(timezone.utc)
        db_session.add(
            CkanDataJob(
                resource_id="res-1",
                dataset_name="ds-1",
                idempotency_key="ik-1",
                instance_id="inst-1",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            )
        )
        db_session.add(
            LatestResourceJob(
                resource_id="res-1",
                latest_job_id="job-1",
                instance_id="inst-1",
                dataset_name="ds-1",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            )
        )
        await db_session.flush()

        stats = await _query_dashboard_stats(db_session)
        assert len(stats) == 1
        assert stats[0]["empty"] == 0

    async def test_empty_count_with_labels(self, db_session):
        """Dashboard stats count resources with the 'empty' label by instance."""
        inst1 = CkanInstance(
            id="inst-a",
            name="Instance A",
            url="https://a.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        inst2 = CkanInstance(
            id="inst-b",
            name="Instance B",
            url="https://b.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add_all([inst1, inst2])
        await db_session.flush()

        now = datetime.now(timezone.utc)
        # Instance A: 2 resources (both empty)
        db_session.add_all([
            CkanDataJob(
                resource_id="res-a1",
                dataset_name="ds-a",
                idempotency_key="ik-a1",
                instance_id="inst-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            CkanDataJob(
                resource_id="res-a2",
                dataset_name="ds-a",
                idempotency_key="ik-a2",
                instance_id="inst-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-a1",
                latest_job_id="job-a1",
                instance_id="inst-a",
                dataset_name="ds-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-a2",
                latest_job_id="job-a2",
                instance_id="inst-a",
                dataset_name="ds-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            ResourceMetadataLabel(resource_id="res-a1", label="empty"),
            ResourceMetadataLabel(resource_id="res-a2", label="empty"),
        ])

        # Instance B: 1 resource (not empty)
        db_session.add_all([
            CkanDataJob(
                resource_id="res-b1",
                dataset_name="ds-b",
                idempotency_key="ik-b1",
                instance_id="inst-b",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-b1",
                latest_job_id="job-b1",
                instance_id="inst-b",
                dataset_name="ds-b",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
        ])
        await db_session.flush()

        stats = await _query_dashboard_stats(db_session)
        assert len(stats) == 2

        # Sort by instance_id for deterministic assertions
        stats.sort(key=lambda s: s["instance_id"])

        assert stats[0]["instance_id"] == "inst-a"
        assert stats[0]["empty"] == 2
        assert stats[0]["completed"] == 2

        assert stats[1]["instance_id"] == "inst-b"
        assert stats[1]["empty"] == 0
        assert stats[1]["completed"] == 1

    async def test_empty_count_not_affected_by_other_labels(self, db_session):
        """Only 'empty' label is counted; other labels are ignored."""
        inst = CkanInstance(
            id="inst-1",
            name="Instance 1",
            url="https://example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(inst)
        await db_session.flush()

        now = datetime.now(timezone.utc)
        db_session.add_all([
            CkanDataJob(
                resource_id="res-1",
                dataset_name="ds-1",
                idempotency_key="ik-1",
                instance_id="inst-1",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-1",
                latest_job_id="job-1",
                instance_id="inst-1",
                dataset_name="ds-1",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            # res-1 has "empty" AND "stale" labels — should still count as 1 empty
            ResourceMetadataLabel(resource_id="res-1", label="empty"),
            ResourceMetadataLabel(resource_id="res-1", label="stale"),
        ])
        await db_session.flush()

        stats = await _query_dashboard_stats(db_session)
        assert len(stats) == 1
        assert stats[0]["empty"] == 1
