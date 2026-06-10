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
        from datetime import datetime, timezone

        from sqlalchemy import select

        from ingestor_orchestrator.db import async_session
        from ingestor_orchestrator.models import CkanInstance
        from ingestor_orchestrator.services.metadata_sync import (
            sync_metadata_for_instance,
        )

        async with async_session() as db:
            instance_result = await db.execute(select(CkanInstance))
            instances = instance_result.scalars().all()

            for inst in instances:
                logger.info(f"Syncing CKAN metadata for {inst.name}...")
                try:
                    result = await asyncio.to_thread(
                        sync_metadata_for_instance,
                        instance_id=inst.id,
                        instance_name=inst.name,
                        instance_url=inst.url,
                    )
                    inst.last_metadata_synced = datetime.now(timezone.utc)
                    inst.dataset_count = result.get("dataset_count", 0)
                    inst.resource_count = result.get("resource_count", 0)
                    await db.commit()
                    logger.info(
                        f"Sync done for {inst.name}: {result['dataset_count']} datasets, {result['resource_count']} resources"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to sync instance {inst.name}: {e}", exc_info=True
                    )

                await self._enqueue(inst)

    async def _enqueue(self, instance):
        from ingestor_orchestrator.services.metadata_sync import (
            enqueue_outdated_resources,
        )

        count = await enqueue_outdated_resources(instance.id, instance.name)
        logger.info(f"Enqueued {count} jobs for {instance.name}")


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
