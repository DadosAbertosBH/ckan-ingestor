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
"""Tests for the latest_resource_job table — model and service logic."""

import pytest
import pytest_asyncio
from ingestor_orchestrator.dto import JobCreate
from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanInstance,
    JobStatus,
    LatestResourceJob,
)
from ingestor_orchestrator.services.job_service import JobService
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def instance(engine, _create_tables):
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        inst = CkanInstance(
            id="inst-lrj",
            name="LRJ Test",
            url="https://test.example.com",
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


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------
class TestLatestResourceJobModel:
    async def test_create_latest_resource_job(self, sess, instance):
        """A LatestResourceJob row can be created."""
        job = CkanDataJob(
            resource_id="r-lrj-1",
            dataset_name="ds1",
            idempotency_key="r-lrj-1",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        lrj = LatestResourceJob(
            resource_id="r-lrj-1",
            latest_job_id=job.id,
            instance_id=instance.id,
            dataset_name="ds1",
            status=JobStatus.PENDING,
        )
        sess.add(lrj)
        await sess.flush()

        assert lrj.resource_id == "r-lrj-1"
        assert lrj.latest_job_id == job.id
        assert lrj.status == JobStatus.PENDING
        assert lrj.created_at is not None
        assert lrj.updated_at is not None

    async def test_resource_id_is_primary_key(self, sess, instance):
        """resource_id is the PK — duplicate inserts raise."""
        job = CkanDataJob(
            resource_id="r-dup",
            dataset_name="ds-dup",
            idempotency_key="r-dup",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        lrj1 = LatestResourceJob(
            resource_id="r-dup",
            latest_job_id=job.id,
            instance_id=instance.id,
            dataset_name="ds-dup",
            status=JobStatus.PENDING,
        )
        sess.add(lrj1)
        await sess.flush()

        # Expunge so the session doesn't warn about identity conflict
        # when we create a second instance with the same PK.
        sess.expunge(lrj1)

        lrj2 = LatestResourceJob(
            resource_id="r-dup",
            latest_job_id=job.id,
            instance_id=instance.id,
            dataset_name="ds-dup",
            status=JobStatus.COMPLETED,
        )
        sess.add(lrj2)
        with pytest.raises(Exception):
            await sess.flush()

    async def test_denormalized_fields_stored(self, sess, instance):
        """resource_name, resource_url, resource_format are stored."""
        job = CkanDataJob(
            resource_id="r-fields",
            resource_name="My Resource",
            resource_url="https://example.com/data.csv",
            resource_format="CSV",
            dataset_name="ds-fields",
            idempotency_key="r-fields",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        lrj = LatestResourceJob(
            resource_id="r-fields",
            latest_job_id=job.id,
            instance_id=instance.id,
            resource_name="My Resource",
            resource_url="https://example.com/data.csv",
            resource_format="CSV",
            dataset_name="ds-fields",
            status=JobStatus.PENDING,
        )
        sess.add(lrj)
        await sess.flush()

        fetched = await sess.get(LatestResourceJob, "r-fields")
        assert fetched.resource_name == "My Resource"
        assert fetched.resource_url == "https://example.com/data.csv"
        assert fetched.resource_format == "CSV"

    async def test_job_relationship(self, sess, instance):
        """LatestResourceJob.job relationship resolves to CkanDataJob."""
        job = CkanDataJob(
            resource_id="r-rel",
            dataset_name="ds-rel",
            idempotency_key="r-rel",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        lrj = LatestResourceJob(
            resource_id="r-rel",
            latest_job_id=job.id,
            instance_id=instance.id,
            dataset_name="ds-rel",
            status=JobStatus.PENDING,
        )
        sess.add(lrj)
        await sess.flush()

        assert lrj.job is not None
        assert lrj.job.id == job.id


# ---------------------------------------------------------------------------
# Service upsert tests
# ---------------------------------------------------------------------------
class TestJobServiceUpsertsLatestResource:
    async def test_create_job_inserts_latest_resource(self, sess, instance):
        """Creating a job inserts a LatestResourceJob row."""
        service = JobService(sess)
        jc = JobCreate(
            resource_id="r-new",
            dataset_name="ds-new",
            resource_name="New Resource",
            resource_format="CSV",
            instance_id=instance.id,
        )
        job = await service.create_job(jc)

        lrj = await sess.get(LatestResourceJob, "r-new")
        assert lrj is not None
        assert lrj.latest_job_id == job.id
        assert lrj.resource_name == "New Resource"
        assert lrj.resource_format == "CSV"
        assert lrj.dataset_name == "ds-new"
        assert lrj.status == JobStatus.PENDING
        assert lrj.instance_id == instance.id

    async def test_second_job_updates_latest_resource(self, sess, instance):
        """Creating a second job for the same resource updates LatestResourceJob."""
        service = JobService(sess)
        jc = JobCreate(
            resource_id="r-update",
            dataset_name="ds-update",
            instance_id=instance.id,
        )
        job1 = await service.create_job(jc)

        # Complete first job
        job1.status = JobStatus.COMPLETED
        await sess.commit()

        job2 = await service.create_job(jc)

        lrj = await sess.get(LatestResourceJob, "r-update")
        assert lrj is not None
        assert lrj.latest_job_id == job2.id
        assert lrj.status == JobStatus.PENDING

    async def test_upsert_updates_denormalized_fields(self, sess, instance):
        """Upsert updates resource_name etc. when a new job has different values."""
        service = JobService(sess)
        jc1 = JobCreate(
            resource_id="r-rename",
            dataset_name="ds-rename",
            resource_name="Old Name",
            resource_format="CSV",
            instance_id=instance.id,
        )
        await service.create_job(jc1)

        jc2 = JobCreate(
            resource_id="r-rename",
            dataset_name="ds-rename",
            resource_name="New Name",
            resource_format="JSON",
            instance_id=instance.id,
        )
        await service.create_job(jc2)

        lrj = await sess.get(LatestResourceJob, "r-rename")
        assert lrj.resource_name == "New Name"
        assert lrj.resource_format == "JSON"

    async def test_process_job_updates_latest_status_to_completed(self, sess, instance):
        """Processing a job updates LatestResourceJob status to COMPLETED."""
        service = JobService(sess)
        jc = JobCreate(
            resource_id="r-proc",
            dataset_name="ds-proc",
            instance_id=instance.id,
        )
        await service.create_job(jc)

        lrj = await sess.get(LatestResourceJob, "r-proc")
        assert lrj.status == JobStatus.PENDING

        # Simulate processing — we can't call process_job because it needs
        # real ingestion. Instead, directly update and call the upsert logic.
        # We'll test that the service method to update latest status works.

        # Get the job
        job = await sess.get(CkanDataJob, lrj.latest_job_id)
        job.status = JobStatus.PROCESSING
        job.started_at = job.updated_at
        await sess.flush()

        # Simulate completion by calling _update_latest_resource_status
        job.status = JobStatus.COMPLETED
        await service._update_latest_resource_status(job)

        lrj = await sess.get(LatestResourceJob, "r-proc")
        assert lrj.status == JobStatus.COMPLETED

    async def test_process_job_updates_latest_status_to_failed(self, sess, instance):
        """Failing a job updates LatestResourceJob status to FAILED."""
        service = JobService(sess)
        jc = JobCreate(
            resource_id="r-fail",
            dataset_name="ds-fail",
            instance_id=instance.id,
        )
        await service.create_job(jc)

        job = await sess.get(
            CkanDataJob, (await sess.get(LatestResourceJob, "r-fail")).latest_job_id
        )
        job.status = JobStatus.FAILED
        await service._update_latest_resource_status(job)

        lrj = await sess.get(LatestResourceJob, "r-fail")
        assert lrj.status == JobStatus.FAILED

    async def test_update_latest_resource_status_noop_for_unknown_resource(self, sess):
        """_update_latest_resource_status does nothing if resource_id not in table."""
        service = JobService(sess)
        # Create a bare job (no LatestResourceJob entry for its resource)

        fake_job = CkanDataJob(
            resource_id="nonexistent",
            dataset_name="ds-ghost",
            idempotency_key="nonexistent",
            instance_id="inst-lrj",
            status=JobStatus.COMPLETED,
        )
        # Should not raise even though no LatestResourceJob row exists
        await service._update_latest_resource_status(fake_job)


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------
class TestResourceSchemas:
    async def test_resource_response_from_model(self, sess, instance):
        """ResourceResponse can be constructed from a LatestResourceJob."""
        from ingestor_orchestrator.dto import ResourceResponse

        job = CkanDataJob(
            resource_id="r-schema",
            dataset_name="ds-schema",
            idempotency_key="r-schema",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        lrj = LatestResourceJob(
            resource_id="r-schema",
            latest_job_id=job.id,
            instance_id=instance.id,
            dataset_name="ds-schema",
            status=JobStatus.COMPLETED,
        )
        sess.add(lrj)
        await sess.flush()

        resp = ResourceResponse(
            resource_id=lrj.resource_id,
            resource_name=lrj.resource_name,
            resource_url=lrj.resource_url,
            resource_format=lrj.resource_format,
            dataset_name=lrj.dataset_name,
            status=lrj.status,
            instance_id=lrj.instance_id,
            ckan_resource_url=f"{instance.url}/dataset/ds-schema/resource/r-schema",
            labels=[],
            job_count=1,
            created_at=lrj.created_at,
            updated_at=lrj.updated_at,
        )
        assert resp.resource_id == "r-schema"
        assert resp.status == JobStatus.COMPLETED
        assert resp.job_count == 1

    async def test_resource_detail_response(self, sess, instance):
        """ResourceDetailResponse includes jobs list."""
        from ingestor_orchestrator.dto import (
            ResourceDetailResponse,
        )

        resp = ResourceDetailResponse(
            resource_id="r-detail",
            resource_name=None,
            resource_url=None,
            resource_format=None,
            dataset_name="ds-detail",
            status=JobStatus.PENDING,
            instance_id=instance.id,
            ckan_resource_url="",
            labels=[],
            job_count=2,
            created_at=instance.created_at,
            updated_at=instance.updated_at,
            latest_job=None,
            jobs=[],
        )
        assert resp.job_count == 2
        assert resp.latest_job is None
        assert resp.jobs == []
        assert resp.preview == []

    async def test_resource_detail_with_preview(self, sess, instance):
        """ResourceDetailResponse includes preview from latest successful result."""
        from ingestor_orchestrator.dto import ResourceDetailResponse

        preview_data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        resp = ResourceDetailResponse(
            resource_id="r-preview",
            resource_name=None,
            resource_url=None,
            resource_format="CSV",
            dataset_name="ds-preview",
            status=JobStatus.COMPLETED,
            instance_id=instance.id,
            ckan_resource_url="",
            labels=[],
            job_count=1,
            created_at=instance.created_at,
            updated_at=instance.updated_at,
            latest_job=None,
            jobs=[],
            preview=preview_data,
        )
        assert resp.preview == preview_data
        assert len(resp.preview) == 2
        assert resp.preview[0]["name"] == "Alice"


class TestExtractPreview:
    async def test_extract_preview_from_successful_result(self, sess, instance):
        """_extract_preview returns preview from a successful result."""
        from ingestor_orchestrator.api.resources import _extract_preview

        job = CkanDataJob(
            resource_id="r-ex",
            dataset_name="ds-ex",
            idempotency_key="r-ex",
            instance_id=instance.id,
            status=JobStatus.COMPLETED,
        )
        sess.add(job)
        await sess.flush()

        from ingestor_orchestrator.models import CkanDataJobResult

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=100,
            dataset_preview=[
                {"col1": "a", "col2": 1},
                {"col1": "b", "col2": 2},
            ],
        )
        sess.add(result)
        await sess.flush()

        # Reload job with results
        await sess.refresh(job, attribute_names=["results"])
        preview = _extract_preview(job)
        assert len(preview) == 2
        assert preview[0]["col1"] == "a"

    async def test_extract_preview_empty_when_no_results(self, sess, instance):
        """_extract_preview returns [] when job has no results."""
        from ingestor_orchestrator.api.resources import _extract_preview

        job = CkanDataJob(
            resource_id="r-no-res",
            dataset_name="ds-no-res",
            idempotency_key="r-no-res",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        # Eagerly load results so _extract_preview doesn't trigger lazy load
        await sess.refresh(job, attribute_names=["results"])
        preview = _extract_preview(job)
        assert preview == []

    async def test_extract_preview_empty_when_job_is_none(self, sess):
        """_extract_preview returns [] when job is None."""
        from ingestor_orchestrator.api.resources import _extract_preview

        assert _extract_preview(None) == []

    async def test_extract_preview_skips_failed_results(self, sess, instance):
        """_extract_preview skips failed results and finds successful one."""
        from ingestor_orchestrator.api.resources import _extract_preview
        from ingestor_orchestrator.models import CkanDataJobResult

        job = CkanDataJob(
            resource_id="r-mixed",
            dataset_name="ds-mixed",
            idempotency_key="r-mixed",
            instance_id=instance.id,
            status=JobStatus.COMPLETED,
        )
        sess.add(job)
        await sess.flush()

        fail_result = CkanDataJobResult(
            job_id=job.id,
            success=False,
            error_message="boom",
        )
        sess.add(fail_result)
        await sess.flush()

        # Reload with results
        await sess.refresh(job, attribute_names=["results"])
        preview = _extract_preview(job)
        assert preview == []

    async def test_extract_preview_with_stored_rows(self, sess, instance):
        """_extract_preview returns whatever is stored in dataset_preview."""
        from ingestor_orchestrator.api.resources import _extract_preview
        from ingestor_orchestrator.models import CkanDataJobResult

        rows = [{"id": i} for i in range(5)]
        job = CkanDataJob(
            resource_id="r-5rows",
            dataset_name="ds-5rows",
            idempotency_key="r-5rows",
            instance_id=instance.id,
            status=JobStatus.COMPLETED,
        )
        sess.add(job)
        await sess.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=100,
            dataset_preview=rows,
        )
        sess.add(result)
        await sess.flush()

        await sess.refresh(job, attribute_names=["results"])
        preview = _extract_preview(job)
        assert len(preview) == 5
