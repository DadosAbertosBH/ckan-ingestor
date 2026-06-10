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
import os
import signal

import nats as nats_lib

from ingestor_orchestrator.config import settings
from ingestor_orchestrator.db import async_session
from ingestor_orchestrator.services.job_service import JobService

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "4"))

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self):
        self._nc = None
        self._running = False
        self._semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)

    async def start(self):
        self._running = True
        self._nc = await nats_lib.connect(settings.nats_url)
        js = self._nc.jetstream()

        # Ensure the stream exists
        try:
            await js.add_stream(
                name=settings.nats_stream,
                subjects=[settings.nats_subject],
            )
        except Exception:
            pass  # Stream already exists

        # Pull consumer — gives us full control over dispatch and flow.
        # We fetch batches and dispatch each message as a background task.
        # The semaphore limits concurrency without blocking the fetch loop.
        psub = await js.pull_subscribe(
            subject=settings.nats_subject,
            durable="ckan-worker",
            stream=settings.nats_stream,
        )

        logger.info(
            f"Worker started (pull, concurrency={WORKER_CONCURRENCY}), waiting for messages..."
        )

        while self._running:
            try:
                msgs = await psub.fetch(batch=WORKER_CONCURRENCY, timeout=5)
                for msg in msgs:
                    asyncio.create_task(self._process(msg))
            except asyncio.TimeoutError:
                continue
            except nats_lib.errors.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Fetch error: {type(e).__name__}: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _process(self, msg):
        async with self._semaphore:
            await self._handle_message(msg)

    async def _handle_message(self, msg):
        try:
            payload = json.loads(msg.data.decode())
            job_id = payload["job_id"]
            ckan_url = payload.get("ckan_url", "")
            logger.info(f"Processing job {job_id}")

            async with async_session() as db:
                service = JobService(db)
                await service.process_job(job_id, ckan_url=ckan_url)

            await msg.ack()
            logger.info(f"Job {job_id} processed successfully")
        except Exception as e:
            logger.error(f"Failed to process message: {e}", exc_info=True)
            await msg.nak(delay=30)  # Retry after 30 seconds

    async def stop(self):
        self._running = False
        if self._nc:
            await self._nc.close()
        logger.info("Worker stopped")


async def run():
    worker = Worker()
    loop = asyncio.get_event_loop()

    def _shutdown():
        logger.info("Worker shutting down...")
        worker._running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    await worker.start()


def run_standalone():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    run_standalone()
