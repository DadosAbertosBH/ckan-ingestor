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
"""Tests for the labels feature — API endpoints and JobService logic."""

import pytest
from ingestor_orchestrator.dto import JobListResponse
from ingestor_orchestrator.models import (
    CkanDataJob,
    JobStatus,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.services.job_service import JobService
from sqlalchemy import select

pytestmark = pytest.mark.asyncio


async def _create_job(
    db_session, resource_id="r1", dataset_name="d1", instance_id="inst-default"
):
    """Helper to create a CkanDataJob directly in the DB for testing."""
    job = CkanDataJob(
        resource_id=resource_id,
        resource_name="Test Resource",
        resource_url=None,
        resource_format="CSV",
        dataset_name=dataset_name,
        idempotency_key=resource_id,
        status=JobStatus.COMPLETED,
        instance_id=instance_id,
    )
    db_session.add(job)
    await db_session.flush()
    return job


class TestLabelResourceModel:
    async def test_create_label(self, db_session):
        """A ResourceMetadataLabel can be created and persisted."""
        lbl = ResourceMetadataLabel(resource_id="r1", label="empty")
        db_session.add(lbl)
        await db_session.flush()

        assert lbl.id is not None
        assert lbl.resource_id == "r1"
        assert lbl.label == "empty"
        assert lbl.created_at is not None

    async def test_unique_constraint_resource_id_and_label(self, db_session):
        """Duplicate resource_id+label raises integrity error."""
        db_session.add(ResourceMetadataLabel(resource_id="r1", label="empty"))
        await db_session.flush()

        db_session.add(ResourceMetadataLabel(resource_id="r1", label="empty"))
        with pytest.raises(Exception):  # IntegrityError
            await db_session.flush()

    async def test_different_labels_same_resource_ok(self, db_session):
        """Same resource_id with different labels is allowed."""
        db_session.add(ResourceMetadataLabel(resource_id="r1", label="empty"))
        db_session.add(ResourceMetadataLabel(resource_id="r1", label="stale"))
        await db_session.flush()  # should not raise

    async def test_same_label_different_resources_ok(self, db_session):
        """Same label on different resource_ids is allowed."""
        db_session.add(ResourceMetadataLabel(resource_id="r1", label="empty"))
        db_session.add(ResourceMetadataLabel(resource_id="r2", label="empty"))
        await db_session.flush()  # should not raise


class TestJobServiceLabelResource:
    async def test_label_resource_creates_label(self, db_session):
        """_label_resource creates a new label for a resource."""
        service = JobService(db_session)
        await service._label_resource("r1", "empty")

        rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == "r1"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].label == "empty"

    async def test_label_resource_is_idempotent(self, db_session):
        """Calling _label_resource with same args twice doesn't duplicate."""
        service = JobService(db_session)
        await service._label_resource("r1", "empty")
        await service._label_resource("r1", "empty")

        rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == "r1",
                        ResourceMetadataLabel.label == "empty",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1


class TestApplyIngestionLabelsSingleColumn:
    async def test_single_column_label_applied(self, db_session):
        """_apply_ingestion_labels creates 'single-column' when column_count=1."""
        service = JobService(db_session)
        await service._apply_ingestion_labels(
            resource_id="r-single-col",
            rows_processed=5,
            column_count=1,
        )
        rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == "r-single-col",
                        ResourceMetadataLabel.label == "single-column",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1

    async def test_no_single_column_when_multiple_columns(self, db_session):
        """_apply_ingestion_labels does NOT create 'single-column' when column_count>1."""
        service = JobService(db_session)
        await service._apply_ingestion_labels(
            resource_id="r-multi-col",
            rows_processed=5,
            column_count=3,
        )
        rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == "r-multi-col",
                        ResourceMetadataLabel.label == "single-column",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 0

    async def test_no_single_column_when_empty_preview(self, db_session):
        """_apply_ingestion_labels does NOT create 'single-column' when column_count=0."""
        service = JobService(db_session)
        await service._apply_ingestion_labels(
            resource_id="r-empty",
            rows_processed=0,
            column_count=0,
        )
        rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == "r-empty",
                        ResourceMetadataLabel.label == "single-column",
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 0


class TestJobSchemaWithLabels:
    async def test_job_response_with_labels(self, db_session, default_instance):
        """JobResponse serialization includes labels from the resource."""
        job_id = "job-1"
        resource_id = "r-labeled"

        job = CkanDataJob(
            id=job_id,
            resource_id=resource_id,
            resource_name="Labeled Resource",
            resource_url=None,
            resource_format="CSV",
            dataset_name="labeled-ds",
            status=JobStatus.COMPLETED,
            idempotency_key=resource_id,
            instance_id=default_instance.id,
        )
        db_session.add(job)

        lbl = ResourceMetadataLabel(resource_id=resource_id, label="empty")
        db_session.add(lbl)
        await db_session.flush()

        # Simulate what list_jobs/get_job does: attach labels to the model
        label_rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == resource_id
                    )
                )
            )
            .scalars()
            .all()
        )
        job.labels = [lbl.label for lbl in label_rows]

        # Schema creation from model
        resp = JobListResponse.model_validate(job)
        assert resp.labels == ["empty"]

    async def test_job_without_labels_has_empty_list(self, db_session):
        """Job without labels gets labels=[]."""
        job = await _create_job(db_session, resource_id="r-no-label")

        label_rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == "r-no-label"
                    )
                )
            )
            .scalars()
            .all()
        )
        job.labels = [lbl.label for lbl in label_rows]

        resp = JobListResponse.model_validate(job)
        assert resp.labels == []

    async def test_job_with_multiple_labels(self, db_session):
        """Multiple labels on the same resource are all included."""
        resource_id = "r-multi"
        job = await _create_job(db_session, resource_id=resource_id)

        db_session.add(ResourceMetadataLabel(resource_id=resource_id, label="empty"))
        db_session.add(ResourceMetadataLabel(resource_id=resource_id, label="stale"))
        await db_session.flush()

        label_rows = (
            (
                await db_session.execute(
                    select(ResourceMetadataLabel).where(
                        ResourceMetadataLabel.resource_id == resource_id
                    )
                )
            )
            .scalars()
            .all()
        )
        job.labels = [lbl.label for lbl in label_rows]

        resp = JobListResponse.model_validate(job)
        assert sorted(resp.labels) == ["empty", "stale"]
