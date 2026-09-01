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
        consumer = await get_iggy_bus().result_consumer()
        logger.info("Iggy result consumer started")

        async def process(message):
            await self._process(message)

        await consumer.consume_messages(process, self._shutdown)

    async def _process(self, message):
        payload = json.loads(message.payload().decode())
        job_id = payload.get("job_id", "unknown")
        status = payload.get("status", "UNKNOWN")
        logger.info(f"Result received: job={job_id} status={status}")

        async with async_session() as db:
            service = JobService(db)
            await service.apply_result(payload)

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
