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
import signal

from ingestor_orchestrator.db import async_session
from ingestor_orchestrator.iggy_queue import get_iggy_bus
from ingestor_orchestrator.services.job_service import JobService

logger = logging.getLogger(__name__)


class ResultConsumer:
    """Consumes Iggy job results and updates the database."""

    INITIAL_RETRY_DELAY_SECONDS = 1
    MAX_RETRY_DELAY_SECONDS = 30

    def __init__(self):
        self._running = False
        self._connected = False
        self._shutdown = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        """Whether the consumer currently has active Iggy subscriptions."""
        return self._connected

    async def start(self):
        self._running = True
        self._shutdown.clear()
        retry_delay = self.INITIAL_RETRY_DELAY_SECONDS

        while self._running:
            try:
                await self._consume_once()
                if self._running:
                    raise ConnectionError("Iggy result consumer stopped unexpectedly")
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                self._connected = False
                await get_iggy_bus().invalidate()
                if not self._running:
                    break
                logger.exception(
                    "Iggy result consumer disconnected; retrying in %s seconds",
                    retry_delay,
                )
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=retry_delay)
                except TimeoutError:
                    pass
                retry_delay = min(retry_delay * 2, self.MAX_RETRY_DELAY_SECONDS)

        self._connected = False

    async def _consume_once(self) -> None:
        bus = get_iggy_bus()
        consumer = await bus.result_consumer()
        metadata_consumer = await bus.metadata_sync_result_consumer()

        async def process(message):
            await self._process(message)

        async def process_metadata(message):
            await self._process_metadata_sync(message)

        tasks = [
            asyncio.ensure_future(consumer.consume_messages(process, self._shutdown)),
            asyncio.ensure_future(
                metadata_consumer.consume_messages(process_metadata, self._shutdown)
            ),
        ]
        self._connected = True
        logger.info("Iggy result consumer started")
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            self._connected = False
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process(self, message):
        payload = json.loads(message.payload().decode())
        job_id = payload.get("job_id", "unknown")
        status = payload.get("status", "UNKNOWN")
        logger.info(f"Result received: job={job_id} status={status}")

        # Coordinator discovery events share the worker result contract. A
        # PENDING event includes the generated job id and the fields required
        # by MySQL; SUCCESS without an id merely confirms an in-flight job.
        if status == "PENDING" and payload.get("resource_id"):
            from ingestor_orchestrator.dto import JobCreate
            async with async_session() as db:
                await JobService(db).create_coordinated_job(
                    payload["job_id"],
                    JobCreate(
                        resource_id=payload["resource_id"],
                        dataset_name=payload.get("dataset_name") or "unknown",
                        resource_name=payload.get("resource_name"),
                        resource_url=payload.get("resource_url"),
                        resource_format=payload.get("resource_format"),
                        instance_id=payload.get("instance_id"),
                        ckan_url=payload.get("ckan_url") or "",
                        datastore_active=payload.get("datastore_active", False),
                    ),
                )
            return
        if status == "SUCCESS" and payload.get("resource_id") and not payload.get("job_id"):
            logger.info("Coordinator skipped in-flight resource %s", payload["resource_id"])
            return

        async with async_session() as db:
            service = JobService(db)
            await service.apply_result(payload)

    async def _process_metadata_sync(self, message):
        """Apply a terminal metadata sync result exactly once."""
        payload = json.loads(message.payload().decode())
        sync_id = payload.get("sync_id")
        if not sync_id:
            logger.error("Metadata sync result missing sync_id")
            return

        from ingestor_orchestrator.models import CkanInstance, MetadataSync
        from ingestor_orchestrator.services.metadata_service import MetadataService
        from ingestor_orchestrator.services.sync_service import SyncService

        async with async_session() as db:
            record = await db.get(MetadataSync, sync_id)
            if record is None:
                logger.error("Metadata sync result references unknown sync %s", sync_id)
                return
            if record.status in {"success", "failure"}:
                logger.info("Ignoring duplicate metadata sync result %s", sync_id)
                return

            sync_service = SyncService(db)
            if payload.get("status") != "success":
                await sync_service.finish_sync(record, payload, status="failure")
                return

            instance = await db.get(CkanInstance, record.instance_id)
            if instance is None:
                await sync_service.finish_sync(record, payload, status="failure")
                return
            await MetadataService(db).update_sync_result(
                instance, payload.get("dataset_count", 0), payload.get("resource_count", 0)
            )
            await sync_service.finish_sync(record, payload)
            logger.info("Metadata sync %s completed; coordinator published resource jobs", sync_id)

    async def stop(self):
        self._running = False
        self._shutdown.set()
        logger.info("Result consumer stopped")


async def run():
    consumer = ResultConsumer()
    loop = asyncio.get_event_loop()

    def _shutdown():
        logger.info("Result consumer shutting down...")
        asyncio.create_task(consumer.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    await consumer.start()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run())
