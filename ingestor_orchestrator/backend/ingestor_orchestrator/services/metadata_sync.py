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
import logging

from ingestor_orchestrator.services.sync_service import SyncService

logger = logging.getLogger(__name__)


def sync_all_instances() -> list[dict]:
    """
    Sync metadata for all CKAN instances registered in MySQL.
    Returns a list of results, one per instance.
    """
    from sqlalchemy import select

    from ingestor_orchestrator.db import async_session
    from ingestor_orchestrator.models import CkanInstance

    results = []

    async def _run():
        async with async_session() as db:
            sync_service = SyncService(db)
            instance_result = await db.execute(select(CkanInstance))
            instances = instance_result.scalars().all()

            for inst in instances:
                try:
                    sync_record = await sync_service.sync_metadata_for_instance(
                        inst.id, inst.name, inst.url
                    )
                    results.append(
                        {
                            "sync_id": sync_record.id,
                            "instance_id": inst.id,
                            "instance_name": inst.name,
                            "status": sync_record.status,
                        }
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to sync instance {inst.name}: {e}", exc_info=True
                    )
                    results.append(
                        {
                            "instance_id": inst.id,
                            "instance_name": inst.name,
                            "error": str(e),
                        }
                    )

            await db.commit()

    import asyncio

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            import threading

            future = concurrent.futures.Future()

            def _run_in_thread():
                new_loop = asyncio.new_event_loop()
                try:
                    result = new_loop.run_until_complete(_run())
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                finally:
                    new_loop.close()

            threading.Thread(target=_run_in_thread, daemon=True).start()
            future.result(timeout=600)
        else:
            loop.run_until_complete(_run())
    except RuntimeError:
        asyncio.run(_run())

    return results
