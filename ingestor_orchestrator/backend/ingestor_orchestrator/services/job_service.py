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
import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import delete, select
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
from ingestor_orchestrator.dto import JobCreate

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
        await self.db.flush()  # get job.id from DB-generated UUID

        # Publish to Kafka
        record_meta = await self._publish_job(
            job.id, job.resource_id, data.ckan_url,
            resource_url=job.resource_url or "",
            resource_format=job.resource_format or "",
        )

        # Store Kafka routing metadata for debugging
        job.kafka_topic = record_meta.topic
        job.kafka_partition = record_meta.partition
        job.kafka_offset = record_meta.offset

        await self._upsert_latest_resource(job)
        await self.db.commit()
        await self.db.refresh(job)

        logger.debug(
            f"Created job {job.id} for resource {data.resource_id}"
        )
        return job

    async def retry_job(self, job_id: str) -> CkanDataJob:
        job = await self.db.get(CkanDataJob, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        if job.status != JobStatus.FAILED:
            raise ValueError(
                f"Can only retry failed jobs, current status: {job.status}"
            )

        # Publish to retry topic
        record_meta = await self._publish_job(
            job.id, job.resource_id, job.ckan_url or "",
            resource_url=job.resource_url or "",
            resource_format=job.resource_format or "",
            retry=True,
        )

        # Store Kafka routing metadata for debugging
        job.kafka_topic = record_meta.topic
        job.kafka_partition = record_meta.partition
        job.kafka_offset = record_meta.offset

        job.status = JobStatus.PENDING
        job.started_at = None
        job.completed_at = None
        job.updated_at = datetime.now(timezone.utc)
        await self._upsert_latest_resource(job)
        await self.db.commit()
        await self.db.refresh(job)

        logger.info(
            f"Retrying job {job.id}"
        )
        return job

    async def _get_job_with_retry(
        self,
        job_id: str,
        max_retries: int = 5,
        base_delay: float = 0.1,
    ) -> CkanDataJob | None:
        """Look up a job with exponential backoff.

        Handles the race condition where a result message (PROCESSING,
        SUCCESS, FAILED) arrives before create_job's transaction commits.
        """
        for attempt in range(max_retries):
            job = await self.db.get(CkanDataJob, job_id)
            if job is not None:
                return job
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Job {job_id} not found, retrying in {delay:.2f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
        return None

    async def apply_result(self, result_data: dict) -> None:
        """Apply a result published by the ingestion worker.

        Receives a dict with the shape published to ckan.ingest.jobs_result.
        No Kafka dependency — the caller is responsible for consuming the topic.

        Retries with exponential backoff when the job isn't found — handles
        the race condition where the result message arrives before the
        create_job transaction is committed.
        """
        job_id = result_data["job_id"]
        status = result_data["status"]

        job = await self._get_job_with_retry(job_id)
        if not job:
            logger.error(f"Job {job_id} not found for result processing")
            return

        if status == "PROCESSING":
            job.status = JobStatus.PROCESSING
            job.started_at = datetime.now(timezone.utc)
            job.completed_at = None
            job.updated_at = datetime.now(timezone.utc)
            await self.db.commit()
            return

        if status == "FAILED":
            error_message = result_data.get("error_message", "")
            result = CkanDataJobResult(
                job_id=job.id,
                success=False,
                error_message=error_message[:16_000],
                error_trace="",
            )
            self.db.add(result)
            job.status = JobStatus.FAILED
            job.completed_at = datetime.now(timezone.utc)
            job.updated_at = datetime.now(timezone.utc)
            await self._update_latest_resource_status(job)
            await self.db.commit()
            logger.error(f"Job {job.id} failed: {error_message}")
            return

        # SUCCESS or "empty"
        rows_processed = result_data.get("rows_processed", 0)
        expected_rows = result_data.get("expected_rows")
        resource_size = result_data.get("resource_size")
        encoding = result_data.get("encoding")
        reader = result_data.get("reader")
        datastore_active = result_data.get("datastore_active", False)
        expected_columns = result_data.get("expected_columns")

        dataset_preview = sanitize_json_preview(result_data.get("preview"))

        result = CkanDataJobResult(
            job_id=job.id,
            success=True,
            dataset_preview=dataset_preview,
            rows_processed=rows_processed,
            expected_rows=expected_rows,
            resource_size=resource_size,
            encoding=encoding,
        )
        self.db.add(result)
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now(timezone.utc)
        logger.info(f"Job {job.id} completed ({rows_processed} rows)")

        if rows_processed == 0:
            await self._label_resource(job.resource_id, "empty")
        else:
            column_count = len(dataset_preview[0]) if dataset_preview else 0
            await self._apply_ingestion_labels(
                resource_id=job.resource_id,
                rows_processed=rows_processed,
                expected_rows=expected_rows,
                resource_size=resource_size,
                encoding=encoding,
                reader=reader,
                datastore_active=datastore_active,
                column_count=column_count,
                expected_columns=expected_columns,
            )

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

    async def _unlabel_resource(self, resource_id: str, label: str) -> None:
        """Remove a label from a resource, if it exists."""
        result = await self.db.execute(
            delete(ResourceMetadataLabel).where(
                ResourceMetadataLabel.resource_id == resource_id,
                ResourceMetadataLabel.label == label,
            )
        )
        if result.rowcount and result.rowcount > 0:
            await self.db.flush()

    async def _apply_ingestion_labels(
        self,
        resource_id: str,
        rows_processed: int | None,
        expected_rows: int | None = None,
        resource_size: int | None = None,
        encoding: str | None = None,
        reader: str | None = None,
        datastore_active: bool = False,
        column_count: int = 0,
        expected_columns: int | None = None,
    ) -> None:
        """Apply labels based on ingestion metadata."""
        # Resource is no longer empty — remove stale label if present
        await self._unlabel_resource(resource_id, "empty")
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

        if reader:
            await self._label_resource(resource_id, f"reader:{reader}")

        # datastore
        if datastore_active:
            await self._label_resource(resource_id, "datastore")

    async def _publish_job(
        self,
        job_id: str,
        resource_id: str,
        ckan_url: str = "",
        resource_url: str = "",
        resource_format: str = "",
        retry: bool = False,
    ):
        """Publish job to Kafka and return RecordMetadata."""
        import asyncio

        from ingestor_orchestrator.kafka import get_kafka_producer

        topic = settings.kafka_topic_retry if retry else settings.kafka_topic
        payload = json.dumps({
            "job_id": job_id,
            "resource_id": resource_id,
            "ckan_url": ckan_url,
            "resource_url": resource_url or "",
            "resource_format": resource_format or "",
        }).encode()

        def _send():
            producer = get_kafka_producer()
            return producer.send(topic, payload).get(timeout=10)

        try:
            return await asyncio.to_thread(_send)
        except Exception as e:
            logger.error(f"Failed to publish job {job_id} to Kafka: {e}")
            raise
