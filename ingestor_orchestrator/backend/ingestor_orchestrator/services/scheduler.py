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
import logging
import signal

from ingestor_orchestrator.config import settings
from ingestor_orchestrator.db import async_session
from ingestor_orchestrator.schemas import JobCreate
from ingestor_orchestrator.services.job_service import JobService

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self):
        self._running = False

    async def start(self):
        self._running = True
        logger.info(
            f"Scheduler started (interval: {settings.scheduler_interval_minutes} min)"
        )
        while self._running:
            try:
                await self._sync_and_enqueue()
            except Exception as e:
                logger.error(f"Scheduler error: {e}", exc_info=True)

            await asyncio.sleep(settings.scheduler_interval_minutes * 60)

    def stop(self):
        self._running = False

    async def _sync_and_enqueue(self):
        """Sync CKAN metadata, then enqueue outdated resources."""
        logger.info("Syncing CKAN metadata...")
        from ingestor_orchestrator.services.metadata_sync import sync_metadata

        result = await asyncio.to_thread(sync_metadata)
        logger.info(
            f"Sync done: {result['dataset_count']} datasets, {result['resource_count']} resources"
        )

        await self._enqueue_outdated_resources()

    async def _enqueue_outdated_resources(self):
        """Find outdated resources and create jobs for them (same logic as Dagster sensor)."""
        from ckan_ingestor.config.ducklake_settings import DucklakeSettings
        from ckan_ingestor.duckdb_ckan_metadata_ingestor import (
            DuckdbCkanMetadataIngestor,
        )
        from ckan_ingestor.duckdb_connection_factory import from_settings

        ducklake_settings = DucklakeSettings()
        conn = from_settings(ducklake_settings)
        try:
            metadata_ingestor = DuckdbCkanMetadataIngestor(conn)
            outdated_ids = metadata_ingestor.get_outdated_resources_id()
            logger.info(f"Found {len(outdated_ids)} outdated resources")

            async with async_session() as db:
                service = JobService(db)
                for resource_id in outdated_ids:
                    try:
                        # Fetch resource metadata for the job record
                        row = conn.execute(
                            "SELECT name, url, format FROM ckan_resource WHERE id = ?",
                            (resource_id,),
                        ).fetchone()
                        resource_name = row[0] if row else None
                        resource_url = row[1] if row else None
                        resource_format = row[2] if row else None

                        await service.create_job(
                            JobCreate(
                                resource_id=resource_id,
                                resource_name=resource_name,
                                resource_url=resource_url,
                                resource_format=resource_format,
                            )
                        )
                    except Exception as e:
                        logger.error(f"Failed to enqueue resource {resource_id}: {e}")
        finally:
            conn.close()


async def run():
    scheduler = Scheduler()
    loop = asyncio.get_event_loop()

    def _shutdown():
        logger.info("Scheduler shutting down...")
        scheduler.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    await scheduler.start()


def run_standalone():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    run_standalone()
