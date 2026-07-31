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
"""Tests for ingestion metadata — expected_rows, resource_size, encoding, labels."""

from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanDataJobResult,
    CkanInstance,
    JobStatus,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.services.job_service import JobService
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def instance(engine, _create_tables):
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        inst = CkanInstance(
            id="inst-meta",
            name="Meta Test",
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
# Model: new columns on CkanDataJobResult
# ---------------------------------------------------------------------------
class TestJobResultMetadataColumns:
    async def test_expected_rows_stored(self, sess, instance):
        """CkanDataJobResult stores expected_rows from CKAN Datastore."""
        job = CkanDataJob(
            resource_id="r-er",
            dataset_name="ds-er",
            idempotency_key="r-er",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=150,
            expected_rows=200,
        )
        sess.add(result)
        await sess.flush()

        assert result.expected_rows == 200

    async def test_resource_size_stored(self, sess, instance):
        """CkanDataJobResult stores resource_size in bytes."""
        job = CkanDataJob(
            resource_id="r-sz",
            dataset_name="ds-sz",
            idempotency_key="r-sz",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=100,
            resource_size=86_411_613,
        )
        sess.add(result)
        await sess.flush()

        assert result.resource_size == 86_411_613

    async def test_encoding_stored(self, sess, instance):
        """CkanDataJobResult stores detected CSV encoding."""
        job = CkanDataJob(
            resource_id="r-enc",
            dataset_name="ds-enc",
            idempotency_key="r-enc",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=50,
            encoding="latin-1",
        )
        sess.add(result)
        await sess.flush()

        assert result.encoding == "latin-1"

    async def test_metadata_columns_nullable(self, sess, instance):
        """New metadata columns are nullable for backward compatibility."""
        job = CkanDataJob(
            resource_id="r-null",
            dataset_name="ds-null",
            idempotency_key="r-null",
            instance_id=instance.id,
        )
        sess.add(job)
        await sess.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=10,
        )
        sess.add(result)
        await sess.flush()

        assert result.expected_rows is None
        assert result.resource_size is None
        assert result.encoding is None


# ---------------------------------------------------------------------------
# Label logic
# ---------------------------------------------------------------------------
class TestIngestionLabels:
    async def _create_job_with_result(
        self,
        sess,
        instance,
        resource_id,
        rows_processed,
        expected_rows=None,
        resource_size=None,
    ):
        """Helper: create a job + result with metadata."""
        job = CkanDataJob(
            resource_id=resource_id,
            dataset_name=f"ds-{resource_id}",
            idempotency_key=resource_id,
            instance_id=instance.id,
            status=JobStatus.COMPLETED,
        )
        sess.add(job)
        await sess.flush()

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            rows_processed=rows_processed,
            expected_rows=expected_rows,
            resource_size=resource_size,
        )
        sess.add(result)
        await sess.flush()
        return job

    async def test_label_single_row(self, sess, instance):
        """Resource with 1 parsed row gets 'single-row' label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-single",
            rows_processed=1,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-single"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "single-row" in labels

    async def test_no_single_row_label_for_more_rows(self, sess, instance):
        """Resource with >1 rows does NOT get 'single-row' label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-multi",
            rows_processed=10,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-multi"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "single-row" not in labels

    async def test_label_row_count_mismatch(self, sess, instance):
        """Resource with different rows_processed vs expected_rows gets label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-mismatch",
            rows_processed=150,
            expected_rows=200,
            resource_size=None,
            encoding=None,
            datastore_active=True,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-mismatch"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "row-count-mismatch" in labels

    async def test_no_mismatch_label_when_matching(self, sess, instance):
        """No mismatch label when rows_processed == expected_rows."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-match",
            rows_processed=200,
            expected_rows=200,
            resource_size=None,
            encoding=None,
            datastore_active=True,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-match"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "row-count-mismatch" not in labels

    async def test_no_mismatch_label_when_expected_unknown(self, sess, instance):
        """No mismatch label when expected_rows is None."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-unknown",
            rows_processed=150,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-unknown"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "row-count-mismatch" not in labels

    async def test_label_size_small(self, sess, instance):
        """Resource < 1MB gets 'size:small' label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-small",
            rows_processed=10,
            expected_rows=None,
            resource_size=500_000,  # 500KB
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-small"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "size:small" in labels

    async def test_label_size_medium(self, sess, instance):
        """Resource between 1MB and 1GB gets 'size:medium' label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-med",
            rows_processed=10,
            expected_rows=None,
            resource_size=50_000_000,  # 50MB
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-med"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "size:medium" in labels

    async def test_label_size_large(self, sess, instance):
        """Resource > 1GB gets 'size:large' label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-large",
            rows_processed=10,
            expected_rows=None,
            resource_size=2_000_000_000,  # 2GB
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-large"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "size:large" in labels

    async def test_no_size_label_when_size_is_none(self, sess, instance):
        """No size label when resource_size is None."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-nosize",
            rows_processed=10,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-nosize"
                    )
                )
            )
            .scalars()
            .all()
        )
        for label in labels:
            assert not label.startswith("size:")

    async def test_label_encoding(self, sess, instance):
        """Resource with non-utf8 encoding gets 'encoding:<enc>' label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-enc",
            rows_processed=10,
            expected_rows=None,
            resource_size=None,
            encoding="latin-1",
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-enc"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "encoding:latin-1" in labels

    async def test_no_encoding_label_for_utf8(self, sess, instance):
        """No encoding label for utf-8 (the default)."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-utf8",
            rows_processed=10,
            expected_rows=None,
            resource_size=None,
            encoding="utf-8",
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-utf8"
                    )
                )
            )
            .scalars()
            .all()
        )
        for label in labels:
            assert not label.startswith("encoding:")

    async def test_multiple_labels_applied(self, sess, instance):
        """Multiple labels can be applied at once."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-multi-label",
            rows_processed=1,
            expected_rows=5,
            resource_size=419,  # < 1MB
            encoding="latin-1",
            datastore_active=True,
        )

        labels = sorted(
            (
                (
                    await sess.execute(
                        select(ResourceMetadataLabel.label).where(
                            ResourceMetadataLabel.resource_id == "r-multi-label"
                        )
                    )
                )
                .scalars()
                .all()
            )
        )
        assert "single-row" in labels
        assert "row-count-mismatch" in labels
        assert "size:small" in labels
        assert "encoding:latin-1" in labels
        assert "datastore" in labels

    async def test_no_datastore_label_when_inactive(self, sess, instance):
        """No 'datastore' label when datastore_active is False/None."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-no-ds",
            rows_processed=10,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=False,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-no-ds"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "datastore" not in labels

    async def test_labels_are_idempotent(self, sess, instance):
        """Calling _apply_ingestion_labels twice doesn't duplicate labels."""
        service = JobService(sess)
        kwargs = dict(
            resource_id="r-idem",
            rows_processed=1,
            expected_rows=None,
            resource_size=100,
            encoding=None,
            datastore_active=False,
        )
        await service._apply_ingestion_labels(**kwargs)
        await service._apply_ingestion_labels(**kwargs)

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-idem"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert labels.count("single-row") == 1
        assert labels.count("size:small") == 1

    # --- column-count-mismatch ---

    async def test_label_column_count_mismatch(self, sess, instance):
        """Resource with different column_count vs expected_columns gets label."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-col-mismatch",
            rows_processed=100,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=True,
            column_count=3,
            expected_columns=5,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-col-mismatch"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "column-count-mismatch" in labels

    async def test_no_column_count_mismatch_when_matching(self, sess, instance):
        """No mismatch label when column_count == expected_columns."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-col-match",
            rows_processed=100,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=True,
            column_count=5,
            expected_columns=5,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-col-match"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "column-count-mismatch" not in labels

    async def test_no_column_count_mismatch_when_expected_unknown(self, sess, instance):
        """No mismatch label when expected_columns is None."""
        service = JobService(sess)
        await service._apply_ingestion_labels(
            resource_id="r-col-unknown",
            rows_processed=100,
            expected_rows=None,
            resource_size=None,
            encoding=None,
            datastore_active=False,
            column_count=3,
            expected_columns=None,
        )

        labels = (
            (
                await sess.execute(
                    select(ResourceMetadataLabel.label).where(
                        ResourceMetadataLabel.resource_id == "r-col-unknown"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert "column-count-mismatch" not in labels


# ---------------------------------------------------------------------------
# Datastore per-instance URL resolution
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestDatastorePerInstanceUrl:
    """_run_ingestion_sync must use the per-instance ckan_url parameter."""

    async def test_uses_ckan_url_param_for_datastore(self):
        import asyncio

        from ckan_ingestor.datastore_reader import DatastoreReader

        service = JobService(MagicMock())

        instance_url = "https://dados.pbh.gov.br"
        global_ckan_url = "https://dados.mg.gov.br"

        # Mock the DuckDB connection
        mock_conn = MagicMock()
        mock_conn.close = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = [5]
        mock_conn.execute.return_value.arrow.return_value = MagicMock()
        mock_conn.execute.return_value.arrow.return_value.read_all.return_value = (
            MagicMock()
        )
        mock_conn.execute.return_value.arrow.return_value.read_all.return_value.to_pylist.return_value = [
            {
                "id": "test-resource-id",
                "datastore_active": True,
                "format": "CSV",
                "url": "http://fake.csv",
                "size": 1000,
            }
        ]

        with (
            patch(
                "ckan_ingestor.duckdb_connection_factory.from_settings",
                return_value=mock_conn,
            ),
            patch(
                "ckan_ingestor.config.ducklake_settings.DucklakeSettings",
                return_value=MagicMock(ckan_url=global_ckan_url),
            ),
            patch(
                "ckan_ingestor.datastore_reader.DatastoreReader",
                wraps=DatastoreReader,
            ) as mock_reader_cls,
            patch(
                "ckan_ingestor.duckdb_ckan_data_ingestor.DuckdbCkanDataIngestor",
            ),
            patch(
                "ckan_ingestor.s3_pdf_ingestor.S3DocumentIngestor",
            ),
            patch(
                "ckan_ingestor.csv_reader.DuckDbCsvReader",
            ),
        ):
            await asyncio.to_thread(
                service._run_ingestion_sync, "test-resource-id", instance_url
            )

            urls_used = [
                call.kwargs.get("datastore_url", call.args[0] if call.args else None)
                for call in mock_reader_cls.call_args_list
            ]
            expected_url = f"{instance_url.rstrip('/')}/datastore/dump"
            assert expected_url in urls_used, (
                f"DatastoreReader should use instance URL ({expected_url}), "
                f"but got {urls_used}"
            )


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------
class TestJobResultSchemaWithMetadata:
    async def test_job_result_with_metadata(self, sess, instance):
        """JobResultResponse includes new metadata fields."""
        from ingestor_orchestrator.dto import JobResultResponse

        resp = JobResultResponse(
            id="res-1",
            job_id="job-1",
            success=True,
            error_message=None,
            error_trace=None,
            dataset_preview=None,
            rows_processed=150,
            expected_rows=200,
            resource_size=50_000_000,
            encoding="latin-1",
            created_at=instance.created_at,
        )
        assert resp.rows_processed == 150
        assert resp.expected_rows == 200
        assert resp.resource_size == 50_000_000
        assert resp.encoding == "latin-1"

    async def test_job_result_metadata_defaults_to_none(self, sess, instance):
        """New metadata fields default to None for backward compat."""
        from ingestor_orchestrator.dto import JobResultResponse

        resp = JobResultResponse(
            id="res-2",
            job_id="job-2",
            success=True,
            error_message=None,
            error_trace=None,
            dataset_preview=None,
            rows_processed=10,
            created_at=instance.created_at,
        )
        assert resp.expected_rows is None
        assert resp.resource_size is None
        assert resp.encoding is None
