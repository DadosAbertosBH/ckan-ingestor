import asyncio
import json
import logging
import os
import signal

import nats as nats_lib

from ckan_orchestrator.config import settings

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "4"))
from ckan_orchestrator.db import async_session
from ckan_orchestrator.services.job_service import JobService

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

        # Push subscriber — NATS delivers messages individually via callback.
        # Each message is acked/nacked independently, so a stuck job won't block others.
        await js.subscribe(
            subject=settings.nats_subject,
            queue="ckan-workers",
            durable="ckan-worker",
            stream=settings.nats_stream,
            cb=self._on_message,
            manual_ack=True,
        )

        logger.info("Worker started, waiting for messages...")

        # Keep the event loop alive until stopped
        while self._running:
            await asyncio.sleep(1)

    async def _on_message(self, msg):
        async with self._semaphore:
            await self._handle_message(msg)

    async def _handle_message(self, msg):
        try:
            payload = json.loads(msg.data.decode())
            job_id = payload["job_id"]
            logger.info(f"Processing job {job_id}")

            async with async_session() as db:
                service = JobService(db)
                await service.process_job(job_id)

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
