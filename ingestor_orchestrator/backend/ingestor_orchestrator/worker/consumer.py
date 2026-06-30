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

from confluent_kafka import TopicPartition

from ingestor_orchestrator.config import settings
from ingestor_orchestrator.db import async_session
from ingestor_orchestrator.kafka_queue import create_consumer
from ingestor_orchestrator.services.job_service import JobService

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "4"))

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self):
        self._running = False
        self._semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)

    async def start(self):
        self._running = True

        retry_consumer = create_consumer(
            settings.kafka_topic_retry, settings.kafka_group_id + "-retry"
        )
        main_consumer = create_consumer(settings.kafka_topic, settings.kafka_group_id)

        logger.info(
            f"Worker started (concurrency={WORKER_CONCURRENCY}), "
            f"topics: {settings.kafka_topic}, {settings.kafka_topic_retry}"
        )

        await asyncio.gather(
            self._consume_loop(retry_consumer, 1.0),
            self._consume_loop(main_consumer, 5.0),
        )

    async def _consume_loop(self, consumer, timeout):
        loop = asyncio.get_event_loop()
        pending: set[asyncio.Task] = set()

        while self._running:
            # confluent-kafka v2.15+ — queue support handles polling efficiently
            msg = await loop.run_in_executor(None, consumer.poll, timeout)

            if msg is None or msg.error():
                continue

            async with self._semaphore:
                task = asyncio.create_task(self._process(msg, consumer))
                pending.add(task)

            if pending:
                done, pending = await asyncio.wait(pending, timeout=0)
                for t in done:
                    exc = t.exception()
                    if exc:
                        logger.error("Task failed", exc_info=exc)

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _process(self, msg, consumer):
        payload = json.loads(msg.value().decode())
        job_id = payload["job_id"]
        ckan_url = payload.get("ckan_url", "")
        logger.info(f"Processing job {job_id}")

        async with async_session() as db:
            service = JobService(db)
            await service.process_job(job_id, ckan_url=ckan_url)

        def _commit():
            consumer.commit(
                offsets=[
                    TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                ],
                asynchronous=False,
            )

        await asyncio.get_event_loop().run_in_executor(None, _commit)
        logger.info(f"Job {job_id} processed successfully")

    async def stop(self):
        self._running = False
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
