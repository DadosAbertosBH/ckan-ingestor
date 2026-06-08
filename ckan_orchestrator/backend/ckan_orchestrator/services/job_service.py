import asyncio
import json
import logging
import traceback
from datetime import date, datetime, timezone

import nats as nats_lib
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ckan_orchestrator.config import settings
from ckan_orchestrator.json_utils import sanitize_json_preview
from ckan_orchestrator.models import CkanDataJob, CkanDataJobResult, JobStatus
from ckan_orchestrator.schemas import JobCreate

logger = logging.getLogger(__name__)


class JobService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_job(self, data: JobCreate) -> CkanDataJob:
        idempotency_key = data.resource_id

        # Idempotency: skip if pending or processing job exists for same resource
        existing = await self.db.execute(
            select(CkanDataJob).where(
                CkanDataJob.idempotency_key == idempotency_key,
                CkanDataJob.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]),
            )
        )
        existing_job = existing.scalar_one_or_none()
        if existing_job:
            logger.info(
                f"Job already exists for resource {data.resource_id} (id={existing_job.id})"
            )
            return existing_job

        job = CkanDataJob(
            resource_id=data.resource_id,
            resource_name=data.resource_name,
            resource_url=data.resource_url,
            resource_format=data.resource_format,
            dataset_name=data.dataset_name,
            idempotency_key=idempotency_key,
            status=JobStatus.PENDING,
        )
        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)

        # Publish to NATS
        await self._publish_job(job.id)

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
        await self.db.commit()
        await self.db.refresh(job)

        await self._publish_job(job.id)
        logger.info(f"Retrying job {job.id}")
        return job

    async def process_job(self, job_id: str) -> None:
        """Called by the worker to process a job."""
        job = await self.db.get(CkanDataJob, job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return

        job.status = JobStatus.PROCESSING
        job.started_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)
        await self.db.commit()

        try:
            rows_processed, preview = await asyncio.to_thread(
                self._run_ingestion_sync, job.resource_id
            )
            result = CkanDataJobResult(
                job_id=job.id,
                success=True,
                dataset_preview=self._sanitize_preview(preview),
                rows_processed=rows_processed,
            )
            self.db.add(result)
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(timezone.utc)
            logger.info(f"Job {job.id} completed successfully ({rows_processed} rows)")
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
        await self.db.commit()

    def _run_ingestion_sync(self, resource_id: str) -> tuple[int, list[dict]]:
        """Synchronous ingestion — runs in a thread pool."""
        from ckan_ingestor.config.ducklake_settings import DucklakeSettings
        from ckan_ingestor.csv_reader import DuckDbCsvReader
        from ckan_ingestor.datastore_reader import DatastoreReader
        from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
        from ckan_ingestor.duckdb_connection_factory import from_settings
        from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor

        ducklake_settings = DucklakeSettings()
        conn = from_settings(ducklake_settings)

        try:
            ingestor = DuckdbCkanDataIngestor(
                ducklake_conn=conn,
                document_ingestor=S3DocumentIngestor(ducklake_settings.data_path),
                datastore_reader=DatastoreReader(ducklake_settings.datastore_url),
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
            ingestor.ingest_ckan_data(resource)

            # Get row count and preview
            count = conn.execute(f'SELECT COUNT(*) FROM "{resource_id}"').fetchone()[0]
            preview_rows = (
                conn.execute(f'SELECT * FROM "{resource_id}" LIMIT 5')
                .arrow()
                .read_all()
                .to_pylist()
            )

            return count, preview_rows
        finally:
            conn.close()

    @staticmethod
    def _sanitize_preview(preview: list[dict] | None) -> list[dict] | None:
        """Delegate to sanitize_json_preview for backward compatibility."""
        return sanitize_json_preview(preview)

    async def _publish_job(self, job_id: str) -> None:
        """Publish job to NATS JetStream."""
        try:
            nc = await nats_lib.connect(settings.nats_url)
            js = nc.jetstream()
            await js.publish(
                settings.nats_subject,
                json.dumps({"job_id": job_id}).encode(),
            )
            await nc.close()
        except Exception as e:
            logger.error(f"Failed to publish job {job_id} to NATS: {e}")
            raise
