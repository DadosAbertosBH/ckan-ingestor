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


async def enqueue_outdated_resources(
    instance_id: str, instance_name: str, ckan_url: str = "", db=None
) -> int:
    """Find outdated resources for an instance and create jobs for them."""
    import asyncio

    from ingestor_orchestrator.db import async_session
    from ingestor_orchestrator.ducklake import (
        DucklakeSettings,
        from_settings,
        get_outdated_resource_ids,
    )
    from ingestor_orchestrator.dto import JobCreate
    from ingestor_orchestrator.services.job_service import JobService

    def _get_outdated():
        ducklake_settings = DucklakeSettings()
        conn = from_settings(ducklake_settings)
        try:
            outdated_ids = get_outdated_resource_ids(conn, ckan_url)
            return outdated_ids, conn
        except Exception:
            conn.close()
            raise

    outdated_ids, conn = await asyncio.to_thread(_get_outdated)
    # DuckDB returns UUID-typed resource ids as ``uuid.UUID``. The broker and
    # MySQL job contract use string identifiers.
    outdated_ids = [str(resource_id) for resource_id in outdated_ids]
    logger.info(f"Found {len(outdated_ids)} outdated resources for {instance_name}")
    if not outdated_ids:
        conn.close()
        return 0

    enqueued = 0

    async def _enqueue(db_session):
        nonlocal enqueued
        from sqlalchemy import select

        from ingestor_orchestrator.models import (
            CkanDataJob,
            JobStatus,
            LatestResourceJob,
        )

        # 1) Single MySQL query: all resource_ids already PENDING/PROCESSING
        in_flight = {
            r[0]
            for r in (
                await db_session.execute(
                    select(CkanDataJob.idempotency_key).where(
                        CkanDataJob.status.in_(
                            [JobStatus.PENDING, JobStatus.PROCESSING]
                        )
                    )
                )
            ).fetchall()
        }

        failed_resources = {
            r[0]
            for r in (
                await db_session.execute(
                    select(CkanDataJob.resource_id)
                    .join(
                        LatestResourceJob,
                        LatestResourceJob.latest_job_id == CkanDataJob.id,
                    )
                    .where(
                        LatestResourceJob.resource_id.in_(outdated_ids),
                        CkanDataJob.status == JobStatus.FAILED,
                    )
                )
            ).fetchall()
        }

        # 2) Filter out already in-flight resources
        to_enqueue = [rid for rid in outdated_ids if rid not in in_flight]
        skipped = len(outdated_ids) - len(to_enqueue)
        if skipped:
            logger.info(f"Skipping {skipped} resources already PENDING/PROCESSING")

        if not to_enqueue:
            return

        # 3) Batch query DuckDB for resource metadata
        placeholders = ",".join(["?"] * len(to_enqueue))
        rows = conn.execute(
            f"SELECT r.name, r.url, r.format, d.name AS dataset_name, r.id, "
            f"COALESCE(r.datastore_active, false) AS datastore_active "
            f"FROM ckan_resource r "
            f"JOIN ckan_dataset d ON r.package_id = d.id "
            f"WHERE r.id IN ({placeholders})",
            to_enqueue,
        ).fetchall()
        metadata_map = {str(r[4]): r[:4] + (r[5],) for r in rows}

        # 4) Create jobs
        service = JobService(db_session)
        for resource_id in to_enqueue:
            try:
                row = metadata_map.get(resource_id)
                resource_name = row[0] if row else None
                resource_url = row[1] if row else None
                resource_format = row[2] if row else None
                dataset_name = row[3] if row else "unknown"
                datastore_active = row[4] if row else False

                await service.create_job(
                    JobCreate(
                        resource_id=resource_id,
                        dataset_name=dataset_name,
                        resource_name=resource_name,
                        resource_url=resource_url,
                        resource_format=resource_format,
                        instance_id=instance_id,
                        ckan_url=ckan_url,
                        datastore_active=datastore_active,
                    ),
                    retry=resource_id in failed_resources,
                )
                enqueued += 1
            except Exception as e:
                logger.error(f"Failed to enqueue resource {resource_id}: {e}")

    if db is not None:
        await _enqueue(db)
    else:
        async with async_session() as db_session:
            await _enqueue(db_session)
    conn.close()
    logger.info(f"Enqueued {enqueued} jobs for {instance_name}")
    return enqueued
