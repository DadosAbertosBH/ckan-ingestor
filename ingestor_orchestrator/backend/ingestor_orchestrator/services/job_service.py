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
import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone

import nats as nats_lib
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.config import settings
from ingestor_orchestrator.json_utils import sanitize_json_preview
from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanDataJobResult,
    JobStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
)
from ingestor_orchestrator.schemas import JobCreate

logger = logging.getLogger(__name__)


class JobService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_job(self, data: JobCreate) -> CkanDataJob:
        idempotency_key = data.resource_id

        job = CkanDataJob(
            resource_id=data.resource_id,
            resource_name=data.resource_name,
            resource_url=data.resource_url,
            resource_format=data.resource_format,
            dataset_name=data.dataset_name,
            idempotency_key=idempotency_key,
            status=JobStatus.PENDING,
            instance_id=data.instance_id or "",
            ckan_url=data.ckan_url or "",
        )
        self.db.add(job)
        await self.db.flush()
        await self._upsert_latest_resource(job)
        await self.db.commit()
        await self.db.refresh(job)

        # Publish to NATS
        await self._publish_job(job.id, data.ckan_url)

        logger.info(f"Created job {job.id} for resource {data.resource_id}")
        return job

    async def retry_job(self, job_id: str) -> CkanDataJob:
        job = await self.db.get(CkanDataJob, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        if job.status != JobStatus.FAILED:
            raise ValueError(
                f"Can only retry failed jobs, current status: {job.status}"
            )

        job.status = JobStatus.PENDING
        job.started_at = None
        job.completed_at = None
        job.updated_at = datetime.now(timezone.utc)
        await self._upsert_latest_resource(job)
        await self.db.commit()
        await self.db.refresh(job)

        await self._publish_job(job.id, job.ckan_url or "")
        logger.info(f"Retrying job {job.id}")
        return job

    async def process_job(self, job_id: str, ckan_url: str = "") -> None:
        """Called by the worker to process a job."""
        job = await self.db.get(CkanDataJob, job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return

        job.status = JobStatus.PROCESSING
        job.started_at = datetime.now(timezone.utc)
        job.completed_at = None
        job.updated_at = datetime.now(timezone.utc)
        await self.db.commit()

        try:
            (
                rows_processed,
                preview,
                expected_rows,
                resource_size,
                encoding,
                datastore_active,
                expected_columns,
            ) = await asyncio.to_thread(
                self._run_ingestion_sync, job.resource_id, ckan_url
            )
            result = CkanDataJobResult(
                job_id=job.id,
                success=True,
                dataset_preview=self._sanitize_preview(preview),
                rows_processed=rows_processed,
                expected_rows=expected_rows,
                resource_size=resource_size,
                encoding=encoding,
            )
            self.db.add(result)
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(timezone.utc)
            logger.info(f"Job {job.id} completed successfully ({rows_processed} rows)")

            if rows_processed == 0:
                await self._label_resource(job.resource_id, "empty")
            else:
                column_count = len(preview[0]) if preview else 0
                await self._apply_ingestion_labels(
                    resource_id=job.resource_id,
                    rows_processed=rows_processed,
                    expected_rows=expected_rows,
                    resource_size=resource_size,
                    encoding=encoding,
                    datastore_active=datastore_active,
                    column_count=column_count,
                    expected_columns=expected_columns,
                )
        except Exception as e:
            error_trace = traceback.format_exc()
            result = CkanDataJobResult(
                job_id=job.id,
                success=False,
                error_message=str(e)[:16_000],
                error_trace=error_trace[:16_000],
            )
            self.db.add(result)
            job.status = JobStatus.FAILED
            job.completed_at = datetime.now(timezone.utc)
            logger.error(f"Job {job.id} failed: {e}")

        job.updated_at = datetime.now(timezone.utc)
        await self._update_latest_resource_status(job)
        await self.db.commit()

    async def _upsert_latest_resource(self, job: CkanDataJob) -> None:
        """Create or update the LatestResourceJob entry for a given job."""
        existing = await self.db.get(LatestResourceJob, job.resource_id)
        if existing:
            existing.latest_job_id = job.id
            existing.instance_id = job.instance_id
            existing.resource_name = job.resource_name
            existing.resource_url = job.resource_url
            existing.resource_format = job.resource_format
            existing.dataset_name = job.dataset_name
            existing.status = job.status
            existing.updated_at = datetime.now(timezone.utc)
        else:
            latest = LatestResourceJob(
                resource_id=job.resource_id,
                latest_job_id=job.id,
                instance_id=job.instance_id,
                resource_name=job.resource_name,
                resource_url=job.resource_url,
                resource_format=job.resource_format,
                dataset_name=job.dataset_name,
                status=job.status,
            )
            self.db.add(latest)
        await self.db.flush()

    async def _update_latest_resource_status(self, job: CkanDataJob) -> None:
        """Update the LatestResourceJob status after processing completes."""
        latest = await self.db.get(LatestResourceJob, job.resource_id)
        if latest:
            latest.status = job.status
            latest.resource_name = job.resource_name
            latest.resource_url = job.resource_url
            latest.resource_format = job.resource_format
            latest.dataset_name = job.dataset_name
            latest.updated_at = datetime.now(timezone.utc)

    def _run_ingestion_sync(
        self, resource_id: str, ckan_url: str = ""
    ) -> tuple[int, list[dict], int | None, int | None, str | None, bool, int | None]:
        """Synchronous ingestion — runs in a thread pool.

        ckan_url is the per-instance CKAN base URL (from MySQL instance).
        """
        from ckan_ingestor.config.ducklake_settings import DucklakeSettings
        from ckan_ingestor.csv_reader import DuckDbCsvReader
        from ckan_ingestor.datastore_reader import DatastoreReader
        from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
        from ckan_ingestor.duckdb_connection_factory import from_settings
        from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor

        ducklake_settings = DucklakeSettings()
        conn = from_settings(ducklake_settings)

        try:
            # Build datastore URL from the per-instance ckan_url,
            # falling back to the global setting.
            base_url = ckan_url or ducklake_settings.ckan_url
            datastore_url = f"{base_url.rstrip('/')}/datastore/dump"

            ingestor = DuckdbCkanDataIngestor(
                ducklake_conn=conn,
                document_ingestor=S3DocumentIngestor(ducklake_settings.data_path),
                datastore_reader=DatastoreReader(datastore_url),
                csv_reader=DuckDbCsvReader(conn),
            )

            # Fetch resource metadata from ckan_resource table
            resource_row = (
                conn.execute("SELECT * FROM ckan_resource WHERE id = ?", (resource_id,))
                .arrow()
                .read_all()
                .to_pylist()
            )

            if not resource_row:
                raise ValueError(
                    f"Resource {resource_id} not found in ckan_resource table"
                )

            resource = resource_row[0]

            # Extract metadata before ingestion
            resource_size = resource.get("size")
            datastore_active = resource.get("datastore_active", False)

            # Get expected_rows from Datastore API if available
            expected_rows: int | None = None
            expected_columns: int | None = None
            if datastore_active:
                try:
                    reader = DatastoreReader(datastore_url)
                    expected_rows = reader.get_total(resource_id)
                    expected_columns = reader.get_field_count(resource_id)
                except Exception:
                    logger.warning(
                        f"Could not fetch datastore metadata for {resource_id}"
                    )

            ingested = ingestor.ingest_ckan_data(resource)

            if not ingested:
                return (
                    0,
                    [],
                    expected_rows,
                    resource_size,
                    None,
                    datastore_active,
                    expected_columns,
                )

            # Get row count and preview
            count = conn.execute(f'SELECT COUNT(*) FROM "{resource_id}"').fetchone()[0]
            preview_rows = (
                conn.execute(f'SELECT * FROM "{resource_id}" LIMIT 5')
                .arrow()
                .read_all()
                .to_pylist()
            )

            # Get detected encoding from CSV reader
            encoding = getattr(ingestor.csv_reader, "last_encoding", None)

            return (
                count,
                preview_rows,
                expected_rows,
                resource_size,
                encoding,
                datastore_active,
                expected_columns,
            )
        finally:
            conn.close()

    @staticmethod
    def _sanitize_preview(preview: list[dict] | None) -> list[dict] | None:
        """Delegate to sanitize_json_preview for backward compatibility."""
        return sanitize_json_preview(preview)

    async def _label_resource(self, resource_id: str, label: str) -> None:
        """Attach a label to a resource, idempotently."""
        existing = await self.db.execute(
            select(ResourceMetadataLabel).where(
                ResourceMetadataLabel.resource_id == resource_id,
                ResourceMetadataLabel.label == label,
            )
        )
        if existing.scalar_one_or_none():
            return
        self.db.add(ResourceMetadataLabel(resource_id=resource_id, label=label))
        await self.db.flush()

    async def _apply_ingestion_labels(
        self,
        resource_id: str,
        rows_processed: int | None,
        expected_rows: int | None = None,
        resource_size: int | None = None,
        encoding: str | None = None,
        datastore_active: bool = False,
        column_count: int = 0,
        expected_columns: int | None = None,
    ) -> None:
        """Apply labels based on ingestion metadata."""
        # single-row
        if rows_processed == 1:
            await self._label_resource(resource_id, "single-row")

        # single-column
        if column_count == 1:
            await self._label_resource(resource_id, "single-column")

        # row-count-mismatch
        if (
            expected_rows is not None
            and rows_processed is not None
            and rows_processed != expected_rows
        ):
            await self._label_resource(resource_id, "row-count-mismatch")

        # column-count-mismatch
        if (
            expected_columns is not None
            and column_count > 0
            and column_count != expected_columns
        ):
            await self._label_resource(resource_id, "column-count-mismatch")

        # size labels
        _ONE_MB = 1_000_000
        _ONE_GB = 1_000_000_000
        if resource_size is not None:
            if resource_size < _ONE_MB:
                await self._label_resource(resource_id, "size:small")
            elif resource_size < _ONE_GB:
                await self._label_resource(resource_id, "size:medium")
            else:
                await self._label_resource(resource_id, "size:large")

        # encoding (skip utf-8, the default)
        if encoding and encoding != "utf-8":
            await self._label_resource(resource_id, f"encoding:{encoding}")

        # datastore
        if datastore_active:
            await self._label_resource(resource_id, "datastore")

    async def _publish_job(self, job_id: str, ckan_url: str = "") -> None:
        """Publish job to NATS JetStream."""
        try:
            nc = await nats_lib.connect(settings.nats_url)
            js = nc.jetstream()
            await js.publish(
                settings.nats_subject,
                json.dumps({"job_id": job_id, "ckan_url": ckan_url}).encode(),
            )
            await nc.close()
        except Exception as e:
            logger.error(f"Failed to publish job {job_id} to NATS: {e}")
            raise
