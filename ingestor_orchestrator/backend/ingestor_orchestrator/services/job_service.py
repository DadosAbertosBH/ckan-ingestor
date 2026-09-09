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
    CsvHint,
    JobStatus,
    LatestResourceJob,
    LastTerminalStatus,
    ResourceMetadataLabel,
    TerminalStatus,
)
from ingestor_orchestrator.dto import JobCreate
from ingestor_orchestrator.resource_status import classify_resource_status

logger = logging.getLogger(__name__)


class JobService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_coordinated_job(
        self, job_id: str, data: JobCreate
    ) -> CkanDataJob | None:
        """Persist a job already published by the Rust coordinator.

        The coordinator publishes the PENDING result before the JobMessage, so
        this method deliberately does not send another broker message.
        """
        existing = await self.db.get(CkanDataJob, job_id)
        if existing is not None:
            return existing

        processing_job = await self.db.scalar(
            select(CkanDataJob.id).where(
                CkanDataJob.resource_id == data.resource_id,
                CkanDataJob.status == JobStatus.PROCESSING,
            )
        )
        if processing_job is not None:
            logger.info(
                "Skipping coordinator PENDING job %s for resource %s: already processing",
                job_id,
                data.resource_id,
            )
            return None

        job = CkanDataJob(
            id=job_id,
            resource_id=data.resource_id,
            resource_name=data.resource_name,
            resource_url=data.resource_url,
            resource_format=data.resource_format,
            dataset_name=data.dataset_name,
            idempotency_key=data.resource_id,
            status=JobStatus.PENDING,
            instance_id=data.instance_id or "",
            ckan_url=data.ckan_url or "",
            datastore_active=data.datastore_active,
        )
        self.db.add(job)
        await self.db.flush()
        await self._upsert_latest_resource(job)
        await self.db.commit()
        await self.db.refresh(job)
        return job

    async def retry_job(self, job_id: str) -> CkanDataJob:
        job = await self.db.get(CkanDataJob, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        if job.status != JobStatus.FAILED:
            raise ValueError(
                f"Can only retry failed jobs, current status: {job.status}"
            )

        # Preserve the failed attempt and create a new job for the retry.
        retry_job = CkanDataJob(
            resource_id=job.resource_id,
            resource_name=job.resource_name,
            resource_url=job.resource_url,
            resource_format=job.resource_format,
            dataset_name=job.dataset_name,
            status=JobStatus.PENDING,
            idempotency_key=job.idempotency_key,
            instance_id=job.instance_id,
            ckan_url=job.ckan_url,
            datastore_active=job.datastore_active,
        )
        self.db.add(retry_job)
        await self.db.flush()

        # Publish to retry topic
        csv_delimiter = await self._get_csv_delimiter(job.resource_id)
        record_meta = await self._publish_job(
            retry_job.id,
            retry_job.resource_id,
            retry_job.ckan_url or "",
            resource_url=retry_job.resource_url or "",
            resource_format=retry_job.resource_format or "",
            csv_delimiter=csv_delimiter,
            datastore_active=retry_job.datastore_active,
            retry=True,
        )

        # Store broker routing metadata for debugging
        retry_job.broker_type = record_meta.broker_type
        retry_job.message_stream = record_meta.stream
        retry_job.message_topic = record_meta.topic
        retry_job.message_partition = record_meta.partition
        retry_job.message_offset = record_meta.offset
        retry_job.updated_at = datetime.now(timezone.utc)
        await self._upsert_latest_resource(retry_job)
        await self.db.commit()
        await self.db.refresh(retry_job)

        logger.info(f"Retrying job {job.id}")
        return retry_job

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
                delay = base_delay * (2**attempt)
                logger.warning(
                    f"Job {job_id} not found, retrying in {delay:.2f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)
        return None

    async def apply_result(self, result_data: dict) -> None:
        """Apply a result published by the ingestion worker.

        Receives a dict with the shape published to ckan.ingest.jobs_result.
        The caller is responsible for consuming the broker topic.

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
            await self._update_latest_resource_status(job)
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
            await self._record_terminal_status(job, successful=False)
            await self._update_latest_resource_status(job)
            await self.db.commit()
            logger.error(f"Job {job.id} failed: {error_message}")
            return

        # SUCCESS or "empty"
        rows_processed = result_data.get("rows_processed", 0)
        expected_rows = result_data.get("expected_rows")
        resource_size = result_data.get("resource_size")
        encoding = result_data.get("encoding")
        csv_strict_mode = result_data.get("csv_strict_mode")
        csv_delimiter = result_data.get("csv_delimiter")
        csv_samples = result_data.get("csv_samples")
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
        if csv_delimiter:
            await self._upsert_csv_hint(job.resource_id, csv_delimiter)
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now(timezone.utc)
        await self._record_terminal_status(job, successful=True)
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
                csv_strict_mode=csv_strict_mode,
                csv_delimiter=csv_delimiter,
                csv_samples=csv_samples,
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
        terminal = await self._get_terminal_status(job.resource_id)
        resource_status = classify_resource_status(
            job.status, terminal.last_terminal_status if terminal else None
        )
        existing = await self.db.get(LatestResourceJob, job.resource_id)
        if existing:
            existing.latest_job_id = job.id
            existing.instance_id = job.instance_id
            existing.resource_name = job.resource_name
            existing.resource_url = job.resource_url
            existing.resource_format = job.resource_format
            existing.dataset_name = job.dataset_name
            existing.status = resource_status
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
                status=resource_status,
            )
            self.db.add(latest)
        await self.db.flush()

    async def _record_terminal_status(
        self, job: CkanDataJob, *, successful: bool
    ) -> None:
        """Persist the latest terminal outcome without losing an older success."""
        now = job.completed_at or datetime.now(timezone.utc)
        terminal = await self._get_terminal_status(job.resource_id)
        if terminal is None:
            terminal = LastTerminalStatus(
                resource_id=job.resource_id,
                last_terminal_job_id=job.id,
                last_terminal_status=(
                    TerminalStatus.COMPLETED if successful else TerminalStatus.FAILED
                ),
                last_terminal_at=now,
                last_successful_job_id=job.id if successful else None,
            )
            self.db.add(terminal)
        else:
            terminal.last_terminal_job_id = job.id
            terminal.last_terminal_status = (
                TerminalStatus.COMPLETED if successful else TerminalStatus.FAILED
            )
            terminal.last_terminal_at = now
            if successful:
                terminal.last_successful_job_id = job.id
        await self.db.flush()

    async def _get_terminal_status(self, resource_id: str) -> LastTerminalStatus | None:
        result = await self.db.execute(
            select(LastTerminalStatus).where(
                LastTerminalStatus.resource_id == resource_id
            )
        )
        return result.scalar_one_or_none()

    async def _update_latest_resource_status(self, job: CkanDataJob) -> None:
        """Update the status when this job is still the resource's latest one."""
        latest = await self.db.get(LatestResourceJob, job.resource_id)
        if latest and latest.latest_job_id == job.id:
            terminal = await self._get_terminal_status(job.resource_id)
            latest.status = classify_resource_status(
                job.status, terminal.last_terminal_status if terminal else None
            )
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

    async def _upsert_csv_hint(self, resource_id: str, delimiter: str) -> None:
        """Create or update the CSV parsing hint for a resource."""
        hint = await self.db.get(CsvHint, resource_id)
        if hint:
            hint.delimiter = delimiter
        else:
            self.db.add(CsvHint(resource_id=resource_id, delimiter=delimiter))
        await self.db.flush()

    async def _get_csv_delimiter(self, resource_id: str) -> str | None:
        hint = await self.db.get(CsvHint, resource_id)
        return hint.delimiter if hint else None

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
        csv_strict_mode: bool | None = None,
        csv_delimiter: str | None = None,
        csv_samples: str | None = None,
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

        if csv_strict_mode is not None:
            strict_mode_label = "true" if csv_strict_mode else "false"
            await self._label_resource(
                resource_id, f"csv-strict-mode:{strict_mode_label}"
            )

        if csv_delimiter:
            await self._label_resource(resource_id, f"csv-delimiter:{csv_delimiter}")

        if csv_samples:
            await self._label_resource(resource_id, f"csv-samples:{csv_samples}")

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
        csv_delimiter: str | None = None,
        datastore_active: bool = False,
        retry: bool = False,
    ):
        """Publish a job to Apache Iggy and return generic routing metadata."""
        from ingestor_orchestrator.iggy_queue import get_iggy_bus

        topic = settings.iggy_topic_retry if retry else settings.iggy_topic
        payload_data = {
            "job_id": job_id,
            "resource_id": resource_id,
            "ckan_url": ckan_url,
            "resource_url": resource_url or "",
            "resource_format": resource_format or "",
            "datastore_active": datastore_active,
        }
        if csv_delimiter:
            payload_data["csv_delimiter"] = csv_delimiter
        payload = json.dumps(payload_data).encode()

        try:
            return await get_iggy_bus().publish(topic, payload, key=job_id)
        except Exception as e:
            logger.error(f"Failed to publish job {job_id} to Iggy: {e}")
            raise
