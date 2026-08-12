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
import signal

from ingestor_orchestrator.config import settings
from ingestor_orchestrator.db import async_session
from ingestor_orchestrator.kafka_queue import create_kafka_consumer
from ingestor_orchestrator.services.job_service import JobService

logger = logging.getLogger(__name__)

RESULT_TOPIC = "ckan.ingest.jobs_result"
RESULT_GROUP_ID = "ckan-result-consumer"


class ResultConsumer:
    """Consumes ckan.ingest.jobs_result and updates the database."""

    def __init__(self):
        self._running = False

    async def start(self):
        self._running = True

        consumer = create_kafka_consumer(RESULT_TOPIC, RESULT_GROUP_ID)

        logger.info(f"Result consumer started, topic: {RESULT_TOPIC}")

        await self._consume_loop(consumer)

    async def _consume_loop(self, consumer):
        loop = asyncio.get_event_loop()

        while self._running:
            records = await loop.run_in_executor(None, consumer.poll, 5000, 10)

            for msg_list in records.values():
                for record in msg_list:
                    await self._process(record, consumer)

    async def _process(self, record, consumer):
        payload = json.loads(record.value.decode())
        job_id = payload.get("job_id", "unknown")
        status = payload.get("status", "UNKNOWN")
        logger.info(f"Result received: job={job_id} status={status}")

        async with async_session() as db:
            service = JobService(db)
            await service.apply_result(payload)

        # Commit offset
        from kafka import OffsetAndMetadata, TopicPartition

        def _commit():
            consumer.commit({
                TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                    record.offset + 1, "", 0
                )
            })

        await asyncio.get_event_loop().run_in_executor(None, _commit)

    async def stop(self):
        self._running = False
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
