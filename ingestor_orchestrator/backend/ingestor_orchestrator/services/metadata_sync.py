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
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def sync_metadata_for_instance(
    instance_id: str, instance_name: str, instance_url: str
) -> dict:
    """
    Fetch CKAN datasets and resources from a specific instance, upsert into DuckLake.
    Also updates the instance metadata in MySQL.
    """
    import pyarrow

    from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
    from ckan_ingestor.config.ducklake_settings import DucklakeSettings
    from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor
    from ckan_ingestor.duckdb_connection_factory import from_settings

    ducklake_settings = DucklakeSettings()
    conn = from_settings(ducklake_settings)

    try:
        fetcher = CkanDatasetFetcher(url=instance_url)
        ingestor = DuckdbCkanMetadataIngestor(conn)

        # 1. Fetch and ingest datasets
        logger.info(f"Fetching CKAN datasets from {instance_name} ({instance_url})...")
        packages = fetcher.fetch()
        dataset_count = packages.num_rows
        logger.info(f"Found {dataset_count} packages for {instance_name}")
        ingestor.ingest_dataset(packages)

        # 2. Extract and ingest resources
        logger.info(f"Ingesting resources for {instance_name}...")
        resources_col = packages["resources"].combine_chunks().flatten()
        resources = pyarrow.Table.from_struct_array(resources_col)
        resource_count = resources.num_rows
        ingestor.ingest_resources(resources)

        logger.info(
            f"Sync complete for {instance_name}: {dataset_count} datasets, {resource_count} resources"
        )

        return {
            "instance_id": instance_id,
            "instance_name": instance_name,
            "dataset_count": dataset_count,
            "resource_count": resource_count,
        }
    finally:
        conn.close()


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
            instance_result = await db.execute(select(CkanInstance))
            instances = instance_result.scalars().all()

            for inst in instances:
                try:
                    result = sync_metadata_for_instance(
                        instance_id=inst.id,
                        instance_name=inst.name,
                        instance_url=inst.url,
                    )
                    # Update MySQL instance metadata
                    inst.last_metadata_synced = datetime.now(timezone.utc)
                    inst.dataset_count = result["dataset_count"]
                    inst.resource_count = result["resource_count"]
                    results.append(result)
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
