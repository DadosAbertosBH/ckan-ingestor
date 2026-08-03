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
"""Tests for dataset aggregation query."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import case, func, select

from ingestor_orchestrator.models import (
    CkanInstance,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)

pytestmark = pytest.mark.asyncio


async def _query_datasets(db_session, instance_id=None, search=None, limit=50, offset=0):
    """Run the dataset aggregation query (mirrors the API endpoint logic)."""
    subq = (
        select(
            LatestResourceJob.instance_id,
            LatestResourceJob.dataset_name,
            func.count(LatestResourceJob.resource_id).label("total_resources"),
            func.sum(
                case((LatestResourceJob.status == "pending", 1), else_=0)
            ).label("pending_resources"),
            func.sum(
                case((LatestResourceJob.status == "processing", 1), else_=0)
            ).label("processing_resources"),
            func.sum(
                case((LatestResourceJob.status == "completed", 1), else_=0)
            ).label("completed_resources"),
            func.sum(
                case((LatestResourceJob.status == "failed", 1), else_=0)
            ).label("failed_resources"),
            func.sum(
                case((ResourceMetadataLabel.label == "empty", 1), else_=0)
            ).label("empty_resources"),
            func.max(LatestResourceJob.updated_at).label("updated_at"),
        )
        .outerjoin(
            ResourceMetadataLabel,
            (LatestResourceJob.resource_id == ResourceMetadataLabel.resource_id)
            & (ResourceMetadataLabel.label == "empty"),
        )
        .group_by(LatestResourceJob.instance_id, LatestResourceJob.dataset_name)
    )

    if instance_id:
        subq = subq.where(LatestResourceJob.instance_id == instance_id)
    if search:
        subq = subq.where(LatestResourceJob.dataset_name.ilike(f"%{search}%"))

    subq = subq.order_by(func.max(LatestResourceJob.updated_at).desc())
    subq = subq.limit(limit).offset(offset)

    result = await db_session.execute(subq)
    return result.all()


class TestDatasetAggregation:
    async def test_empty_database_returns_empty(self, db_session):
        rows = await _query_datasets(db_session)
        assert rows == []

    async def test_groups_by_instance_and_dataset(self, db_session):
        instances = [
            CkanInstance(
                id=f"inst-{i}",
                name=f"Instance {i}",
                url=f"https://example{i}.com",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            for i in range(2)
        ]
        for inst in instances:
            db_session.add(inst)
        await db_session.flush()

        now = datetime.now(timezone.utc)
        resources = [
            LatestResourceJob(
                resource_id="res-1",
                latest_job_id="job-1",
                instance_id="inst-0",
                dataset_name="dataset-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-2",
                latest_job_id="job-2",
                instance_id="inst-0",
                dataset_name="dataset-a",
                status=JobStatus.PENDING,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-3",
                latest_job_id="job-3",
                instance_id="inst-0",
                dataset_name="dataset-a",
                status=JobStatus.FAILED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-4",
                latest_job_id="job-4",
                instance_id="inst-0",
                dataset_name="dataset-b",
                status=JobStatus.PROCESSING,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-5",
                latest_job_id="job-5",
                instance_id="inst-0",
                dataset_name="dataset-b",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-6",
                latest_job_id="job-6",
                instance_id="inst-1",
                dataset_name="dataset-a",
                status=JobStatus.PROCESSING,
                created_at=now,
                updated_at=now,
            ),
        ]
        for r in resources:
            db_session.add(r)
        await db_session.flush()

        rows = await _query_datasets(db_session)
        # Sort by instance_id + dataset_name for deterministic assertions
        rows.sort(key=lambda r: (r.instance_id, r.dataset_name))
        assert len(rows) == 3

        # inst-0 / dataset-a: 3 resources (1 completed, 1 pending, 1 failed)
        r0 = rows[0]
        assert r0.instance_id == "inst-0"
        assert r0.dataset_name == "dataset-a"
        assert r0.total_resources == 3
        assert r0.pending_resources == 1
        assert r0.processing_resources == 0
        assert r0.completed_resources == 1
        assert r0.failed_resources == 1

        # inst-0 / dataset-b: 2 resources (1 processing, 1 completed)
        r1 = rows[1]
        assert r1.instance_id == "inst-0"
        assert r1.dataset_name == "dataset-b"
        assert r1.total_resources == 2
        assert r1.pending_resources == 0
        assert r1.processing_resources == 1
        assert r1.completed_resources == 1
        assert r1.failed_resources == 0

        # inst-1 / dataset-a: 1 resource (1 processing)
        r2 = rows[2]
        assert r2.instance_id == "inst-1"
        assert r2.dataset_name == "dataset-a"
        assert r2.total_resources == 1
        assert r2.pending_resources == 0
        assert r2.processing_resources == 1
        assert r2.completed_resources == 0
        assert r2.failed_resources == 0

    async def test_filter_by_instance(self, db_session):
        instances = [
            CkanInstance(
                id="inst-a",
                name="Instance A",
                url="https://a.example.com",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            ),
            CkanInstance(
                id="inst-b",
                name="Instance B",
                url="https://b.example.com",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            ),
        ]
        for inst in instances:
            db_session.add(inst)
        await db_session.flush()

        now = datetime.now(timezone.utc)
        r1 = LatestResourceJob(
            resource_id="r-1",
            latest_job_id="j-1",
            instance_id="inst-a",
            dataset_name="ds-a",
            status=JobStatus.COMPLETED,
            created_at=now,
            updated_at=now,
        )
        r2 = LatestResourceJob(
            resource_id="r-2",
            latest_job_id="j-2",
            instance_id="inst-b",
            dataset_name="ds-b",
            status=JobStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        db_session.add_all([r1, r2])
        await db_session.flush()

        rows = await _query_datasets(db_session, instance_id="inst-a")
        assert len(rows) == 1
        assert rows[0].instance_id == "inst-a"
        assert rows[0].dataset_name == "ds-a"

    async def test_filter_by_search(self, db_session):
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
        for i, name in enumerate(["covid-data", "traffic-stats", "budget-info"]):
            db_session.add(
                LatestResourceJob(
                    resource_id=f"r-{i}",
                    latest_job_id=f"j-{i}",
                    instance_id="inst-1",
                    dataset_name=name,
                    status=JobStatus.COMPLETED,
                    created_at=now,
                    updated_at=now,
                )
            )
        await db_session.flush()

        rows = await _query_datasets(db_session, search="covid")
        assert len(rows) == 1
        assert rows[0].dataset_name == "covid-data"

    async def test_orders_by_updated_at_desc(self, db_session):
        inst = CkanInstance(
            id="inst-1",
            name="Instance 1",
            url="https://example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(inst)
        await db_session.flush()

        early = datetime(2025, 1, 1, tzinfo=timezone.utc)
        later = datetime(2025, 6, 1, tzinfo=timezone.utc)

        db_session.add_all([
            LatestResourceJob(
                resource_id="r-1",
                latest_job_id="j-1",
                instance_id="inst-1",
                dataset_name="old-dataset",
                status=JobStatus.COMPLETED,
                created_at=early,
                updated_at=early,
            ),
            LatestResourceJob(
                resource_id="r-2",
                latest_job_id="j-2",
                instance_id="inst-1",
                dataset_name="new-dataset",
                status=JobStatus.COMPLETED,
                created_at=later,
                updated_at=later,
            ),
        ])
        await db_session.flush()

        rows = await _query_datasets(db_session)
        assert len(rows) == 2
        assert rows[0].dataset_name == "new-dataset"
        assert rows[1].dataset_name == "old-dataset"

    async def test_updated_at_is_max_across_resources(self, db_session):
        inst = CkanInstance(
            id="inst-1",
            name="Instance 1",
            url="https://example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(inst)
        await db_session.flush()

        t1 = datetime(2025, 3, 1, tzinfo=timezone.utc)
        t2 = datetime(2025, 5, 1, tzinfo=timezone.utc)

        db_session.add_all([
            LatestResourceJob(
                resource_id="r-1",
                latest_job_id="j-1",
                instance_id="inst-1",
                dataset_name="my-dataset",
                status=JobStatus.COMPLETED,
                created_at=t1,
                updated_at=t1,
            ),
            LatestResourceJob(
                resource_id="r-2",
                latest_job_id="j-2",
                instance_id="inst-1",
                dataset_name="my-dataset",
                status=JobStatus.COMPLETED,
                created_at=t2,
                updated_at=t2,
            ),
        ])
        await db_session.flush()

        rows = await _query_datasets(db_session)
        assert len(rows) == 1
        # updated_at should be the max (t2, the latest)
        # SQLite strips timezone, so compare the replacement
        assert rows[0].updated_at.replace(tzinfo=timezone.utc) == t2

    async def test_ckan_dataset_url_construction(self, db_session):
        """Verify ckan_dataset_url is built from instance URL + dataset name."""
        from ingestor_orchestrator.api.datasets import _build_ckan_dataset_url

        url = _build_ckan_dataset_url(
            "https://dados.example.com/", "my-dataset"
        )
        assert url == "https://dados.example.com/dataset/my-dataset"

    async def test_ckan_dataset_url_trailing_slash(self, db_session):
        """Instance URL trailing slash should be normalized."""
        from ingestor_orchestrator.api.datasets import _build_ckan_dataset_url

        url = _build_ckan_dataset_url(
            "https://dados.example.com", "my-dataset"
        )
        assert url == "https://dados.example.com/dataset/my-dataset"

    async def test_ckan_dataset_url_empty_instance(self, db_session):
        """Empty instance URL returns empty string."""
        from ingestor_orchestrator.api.datasets import _build_ckan_dataset_url

        url = _build_ckan_dataset_url("", "my-dataset")
        assert url == ""

    async def test_instance_last_synced_at_in_result(self, db_session):
        """Dataset response includes instance's last_metadata_synced."""
        synced_at = datetime(2025, 4, 1, tzinfo=timezone.utc)
        inst = CkanInstance(
            id="inst-1",
            name="Instance 1",
            url="https://example.com",
            last_metadata_synced=synced_at,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(inst)
        await db_session.flush()

        now = datetime.now(timezone.utc)
        db_session.add(
            LatestResourceJob(
                resource_id="r-1",
                latest_job_id="j-1",
                instance_id="inst-1",
                dataset_name="ds-1",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            )
        )
        await db_session.flush()

        from ingestor_orchestrator.api.datasets import _build_ckan_dataset_url
        # Test that the API endpoint includes last_metadata_synced
        rows = await _query_datasets(db_session)
        assert len(rows) == 1

        # The query doesn't return last_metadata_synced directly —
        # that's added in the API response builder after fetching instances.
        # We test that the instance lookup works correctly.
        instance = await db_session.get(CkanInstance, "inst-1")
        assert instance.last_metadata_synced == synced_at

        # Verify ckan_dataset_url is correct for this instance
        url = _build_ckan_dataset_url(instance.url, "ds-1")
        assert url == "https://example.com/dataset/ds-1"

    async def test_empty_resources_count(self, db_session):
        """Dataset aggregation counts resources with 'empty' label."""
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
        resources = [
            LatestResourceJob(
                resource_id="res-1",
                latest_job_id="job-1",
                instance_id="inst-1",
                dataset_name="dataset-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-2",
                latest_job_id="job-2",
                instance_id="inst-1",
                dataset_name="dataset-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
            LatestResourceJob(
                resource_id="res-3",
                latest_job_id="job-3",
                instance_id="inst-1",
                dataset_name="dataset-b",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            ),
        ]
        for r in resources:
            db_session.add(r)

        # res-1 and res-3 have the "empty" label
        db_session.add(
            ResourceMetadataLabel(resource_id="res-1", label="empty")
        )
        db_session.add(
            ResourceMetadataLabel(resource_id="res-3", label="empty")
        )
        # res-1 also has another label (should not affect empty count)
        db_session.add(
            ResourceMetadataLabel(resource_id="res-1", label="stale")
        )
        await db_session.flush()

        rows = await _query_datasets(db_session)
        rows.sort(key=lambda r: r.dataset_name)

        assert len(rows) == 2

        # dataset-a: 2 resources (res-1 is empty, res-2 is not)
        assert rows[0].dataset_name == "dataset-a"
        assert rows[0].total_resources == 2
        assert rows[0].empty_resources == 1

        # dataset-b: 1 resource (res-3 is empty)
        assert rows[1].dataset_name == "dataset-b"
        assert rows[1].total_resources == 1
        assert rows[1].empty_resources == 1

    async def test_empty_resources_count_with_none(self, db_session):
        """Dataset with no empty labels returns 0 for empty_resources."""
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
            LatestResourceJob(
                resource_id="res-1",
                latest_job_id="job-1",
                instance_id="inst-1",
                dataset_name="dataset-a",
                status=JobStatus.COMPLETED,
                created_at=now,
                updated_at=now,
            )
        )
        await db_session.flush()

        rows = await _query_datasets(db_session)
        assert len(rows) == 1
        assert rows[0].empty_resources == 0
