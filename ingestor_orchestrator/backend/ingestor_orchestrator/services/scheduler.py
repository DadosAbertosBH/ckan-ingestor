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
import logging
import signal

from ingestor_orchestrator.config import settings

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
        """Sync CKAN metadata for all instances, then enqueue outdated resources."""
        from sqlalchemy import select

        from ingestor_orchestrator.db import async_session
        from ingestor_orchestrator.models import CkanInstance
        from ingestor_orchestrator.services.sync_service import SyncService

        async with async_session() as db:
            sync_service = SyncService(db)
            instance_result = await db.execute(select(CkanInstance))
            instances = instance_result.scalars().all()

            for inst in instances:
                logger.info(f"Syncing CKAN metadata for {inst.name}...")
                try:
                    sync_record = await sync_service.sync_metadata_for_instance(
                        inst.id, inst.name, inst.url
                    )
                    logger.info("Metadata sync %s queued for %s", sync_record.id, inst.name)
                except Exception as e:
                    logger.error(
                        f"Failed to sync instance {inst.name}: {e}", exc_info=True
                    )



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
