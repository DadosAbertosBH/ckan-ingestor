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

    def __init__(self):
        self._running = False
        self._shutdown = asyncio.Event()

    async def start(self):
        self._running = True
        self._shutdown.clear()
        bus = get_iggy_bus()
        consumer = await bus.result_consumer()
        metadata_consumer = await bus.metadata_sync_result_consumer()
        logger.info("Iggy result consumer started")

        async def process(message):
            await self._process(message)

        async def process_metadata(message):
            await self._process_metadata_sync(message)

        await asyncio.gather(
            consumer.consume_messages(process, self._shutdown),
            metadata_consumer.consume_messages(process_metadata, self._shutdown),
        )

    async def _process(self, message):
        payload = json.loads(message.payload().decode())
        job_id = payload.get("job_id", "unknown")
        status = payload.get("status", "UNKNOWN")
        logger.info(f"Result received: job={job_id} status={status}")

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
        from ingestor_orchestrator.services.metadata_sync import enqueue_outdated_resources
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
            enqueued = await enqueue_outdated_resources(
                instance.id, instance.name, instance.url, db=db
            )
            logger.info("Metadata sync %s completed; enqueued %s resources", sync_id, enqueued)

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
