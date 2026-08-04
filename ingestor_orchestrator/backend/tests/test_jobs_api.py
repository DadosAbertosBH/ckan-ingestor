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
"""Tests for GET /api/jobs/ — ordering, filtering, tags, and instance_name."""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.main import app
from ingestor_orchestrator.models import (
    CkanDataJob,
    JobStatus,
    ResourceMetadataLabel,
)

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client(db_session):
    """TestClient with get_db overridden to use the in-memory test session."""
    async def override_get_db():
        yield db_session
    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestListJobsInstanceName:
    async def test_list_jobs_includes_instance_name(
        self, db_session, default_instance, client
    ):
        """When listing jobs with an instance, instance_name is included in the response."""
        job = CkanDataJob(
            resource_id="r-inst-name",
            resource_name="Instance Name Test",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-inst-name",
            status=JobStatus.COMPLETED,
            idempotency_key="r-inst-name",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        db_session.add(job)
        await db_session.flush()

        response = client.get("/api/jobs/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["instance_name"] == "Default"


class TestListJobsOrderBy:
    async def test_list_jobs_order_by_created_at_asc(
        self, db_session, default_instance, client
    ):
        """order_by=created_at with order_dir=asc returns jobs in ascending order."""
        now = datetime.now(timezone.utc)

        jobs_data = [
            ("r-mid", now - timedelta(hours=1)),
            ("r-oldest", now - timedelta(hours=3)),
            ("r-newest", now - timedelta(minutes=5)),
        ]
        for resource_id, created_at in jobs_data:
            job = CkanDataJob(
                resource_id=resource_id,
                resource_name=f"Resource {resource_id}",
                resource_url=None,
                resource_format="CSV",
                dataset_name="ds-order",
                status=JobStatus.COMPLETED,
                idempotency_key=resource_id,
                instance_id=default_instance.id,
                ckan_url=default_instance.url,
                created_at=created_at,
            )
            db_session.add(job)
        await db_session.flush()

        response = client.get("/api/jobs/?order_by=created_at&order_dir=asc")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        # oldest first
        assert data[0]["resource_id"] == "r-oldest"
        assert data[1]["resource_id"] == "r-mid"
        assert data[2]["resource_id"] == "r-newest"

    async def test_list_jobs_default_order_is_created_at_desc(
        self, db_session, default_instance, client
    ):
        """Without explicit order params, jobs default to created_at desc."""
        now = datetime.now(timezone.utc)

        jobs_data = [
            ("r-oldest", now - timedelta(hours=3)),
            ("r-newest", now - timedelta(minutes=5)),
            ("r-mid", now - timedelta(hours=1)),
        ]
        for resource_id, created_at in jobs_data:
            job = CkanDataJob(
                resource_id=resource_id,
                resource_name=f"Resource {resource_id}",
                resource_url=None,
                resource_format="CSV",
                dataset_name="ds-default",
                status=JobStatus.COMPLETED,
                idempotency_key=resource_id,
                instance_id=default_instance.id,
                ckan_url=default_instance.url,
                created_at=created_at,
            )
            db_session.add(job)
        await db_session.flush()

        response = client.get("/api/jobs/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        # newest first (desc)
        assert data[0]["resource_id"] == "r-newest"
        assert data[1]["resource_id"] == "r-mid"
        assert data[2]["resource_id"] == "r-oldest"

    async def test_list_jobs_order_by_duration_asc(
        self, db_session, default_instance, client
    ):
        """order_by=duration with order_dir=asc sorts by COALESCE(completed_at, NOW()) - started_at,
        with NULL started_at appearing last."""
        now = datetime.now(timezone.utc)

        # Job with the shortest duration: 1 minute
        j1 = CkanDataJob(
            resource_id="r-short",
            resource_name="Short Duration",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-duration",
            status=JobStatus.COMPLETED,
            idempotency_key="r-short",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
            started_at=now - timedelta(minutes=5),
            completed_at=now - timedelta(minutes=4),
        )
        # Job with longer duration: 5 minutes
        j2 = CkanDataJob(
            resource_id="r-long",
            resource_name="Long Duration",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-duration",
            status=JobStatus.COMPLETED,
            idempotency_key="r-long",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
            started_at=now - timedelta(minutes=10),
            completed_at=now - timedelta(minutes=5),
        )
        # Job with no started_at — should appear last in ascending order
        j3 = CkanDataJob(
            resource_id="r-no-start",
            resource_name="No Started At",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-duration",
            status=JobStatus.PENDING,
            idempotency_key="r-no-start",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
            started_at=None,
            completed_at=None,
        )
        db_session.add_all([j1, j2, j3])
        await db_session.flush()

        response = client.get("/api/jobs/?order_by=duration&order_dir=asc")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        # Shortest duration first
        assert data[0]["resource_id"] == "r-short"
        # Longer duration second
        assert data[1]["resource_id"] == "r-long"
        # NULL started_at last
        assert data[2]["resource_id"] == "r-no-start"

    async def test_list_jobs_order_by_duration_desc(
        self, db_session, default_instance, client
    ):
        """order_by=duration with order_dir=desc puts NULL started_at last, longest first."""
        now = datetime.now(timezone.utc)

        j1 = CkanDataJob(
            resource_id="r-short",
            resource_name="Short",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-duration",
            status=JobStatus.COMPLETED,
            idempotency_key="r-short",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
            started_at=now - timedelta(minutes=5),
            completed_at=now - timedelta(minutes=4),
        )
        j2 = CkanDataJob(
            resource_id="r-long",
            resource_name="Long",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-duration",
            status=JobStatus.COMPLETED,
            idempotency_key="r-long",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
            started_at=now - timedelta(minutes=10),
            completed_at=now - timedelta(minutes=5),
        )
        j3 = CkanDataJob(
            resource_id="r-no-start",
            resource_name="No Start",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-duration",
            status=JobStatus.PENDING,
            idempotency_key="r-no-start",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
            started_at=None,
            completed_at=None,
        )
        db_session.add_all([j1, j2, j3])
        await db_session.flush()

        response = client.get("/api/jobs/?order_by=duration&order_dir=desc")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        # Longest duration first
        assert data[0]["resource_id"] == "r-long"
        assert data[1]["resource_id"] == "r-short"
        assert data[2]["resource_id"] == "r-no-start"


class TestListJobsFilterByTags:
    async def test_list_jobs_filter_by_tags_single(
        self, db_session, default_instance, client
    ):
        """tags=empty returns only jobs whose resource_id has the 'empty' label."""
        job_tagged = CkanDataJob(
            resource_id="r-tagged",
            resource_name="Tagged",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-tags",
            status=JobStatus.COMPLETED,
            idempotency_key="r-tagged",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        job_untagged = CkanDataJob(
            resource_id="r-untagged",
            resource_name="Untagged",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-tags",
            status=JobStatus.COMPLETED,
            idempotency_key="r-untagged",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        db_session.add_all([job_tagged, job_untagged])
        db_session.add(ResourceMetadataLabel(resource_id="r-tagged", label="empty"))
        await db_session.flush()

        response = client.get("/api/jobs/?tags=empty")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["resource_id"] == "r-tagged"

    async def test_list_jobs_filter_by_tags_multiple(
        self, db_session, default_instance, client
    ):
        """tags=empty,stale returns jobs matching ANY of the given labels (OR semantics)."""
        job_empty = CkanDataJob(
            resource_id="r-empty",
            resource_name="Empty Label",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-tags",
            status=JobStatus.COMPLETED,
            idempotency_key="r-empty",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        job_stale = CkanDataJob(
            resource_id="r-stale",
            resource_name="Stale Label",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-tags",
            status=JobStatus.COMPLETED,
            idempotency_key="r-stale",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        job_neither = CkanDataJob(
            resource_id="r-neither",
            resource_name="No Matching Label",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-tags",
            status=JobStatus.COMPLETED,
            idempotency_key="r-neither",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        db_session.add_all([job_empty, job_stale, job_neither])
        db_session.add(ResourceMetadataLabel(resource_id="r-empty", label="empty"))
        db_session.add(ResourceMetadataLabel(resource_id="r-stale", label="stale"))
        await db_session.flush()

        response = client.get("/api/jobs/?tags=empty,stale")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        resource_ids = {item["resource_id"] for item in data}
        assert resource_ids == {"r-empty", "r-stale"}

    async def test_list_jobs_filter_by_tags_no_match_returns_empty(
        self, db_session, default_instance, client
    ):
        """tags with a label that no resource has returns an empty list."""
        job = CkanDataJob(
            resource_id="r-none",
            resource_name="No Label",
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-tags",
            status=JobStatus.COMPLETED,
            idempotency_key="r-none",
            instance_id=default_instance.id,
            ckan_url=default_instance.url,
        )
        db_session.add(job)
        db_session.add(ResourceMetadataLabel(resource_id="r-none", label="stale"))
        await db_session.flush()

        response = client.get("/api/jobs/?tags=empty")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 0
