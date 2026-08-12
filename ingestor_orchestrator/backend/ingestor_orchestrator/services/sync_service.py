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
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.models import MetadataSync

logger = logging.getLogger(__name__)


class SyncService:
    """Service layer for CKAN metadata syncs — records runs and syncs DuckLake."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def start_sync(self, instance_id: str) -> MetadataSync:
        """Create a sync record with start_time set to now."""
        record = MetadataSync(instance_id=instance_id)
        self.db.add(record)
        await self.db.commit()
        await self.db.refresh(record)
        return record

    async def finish_sync(
        self, record: MetadataSync, result: dict, status: str = "success"
    ) -> MetadataSync:
        """Fill in end_time, status, and the new/updated counts for a finished sync."""
        record.end_time = datetime.now(timezone.utc)
        record.status = status
        record.total_packages = result.get("total_packages", 0)
        record.new_datasets = result.get("new_datasets", 0)
        record.new_resources = result.get("new_resources", 0)
        record.updated_datasets = result.get("updated_datasets", 0)
        record.updated_resources = result.get("updated_resources", 0)
        await self.db.commit()
        await self.db.refresh(record)
        logger.info(
            f"Sync record {record.id} saved: "
            f"total={record.total_packages}, new_ds={record.new_datasets}, "
            f"new_rs={record.new_resources}, upd_ds={record.updated_datasets}, "
            f"upd_rs={record.updated_resources}"
        )
        return record

    async def sync_metadata_for_instance(
        self, instance_id: str, instance_name: str, instance_url: str
    ) -> dict:
        """Fetch CKAN datasets and resources from an instance, upsert into DuckLake.

        Dispatches the blocking sync to a thread so the event loop stays responsive.
        """
        return await asyncio.to_thread(
            self._run_metadata_sync, instance_id, instance_name, instance_url
        )

    @staticmethod
    def _run_metadata_sync(
        instance_id: str, instance_name: str, instance_url: str
    ) -> dict:
        """Blocking metadata sync — fetch CKAN data and upsert into DuckLake."""
        import pyarrow

        from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
        from ckan_ingestor.config.ducklake_settings import DucklakeSettings
        from ckan_ingestor.duckdb_ckan_metadata_ingestor import (
            DuckdbCkanMetadataIngestor,
        )
        from ckan_ingestor.duckdb_connection_factory import from_settings

        ducklake_settings = DucklakeSettings()
        conn = from_settings(ducklake_settings)

        try:
            fetcher = CkanDatasetFetcher(url=instance_url)
            ingestor = DuckdbCkanMetadataIngestor(conn)

            # 1. Fetch and ingest datasets
            logger.info(
                f"Fetching CKAN datasets from {instance_name} ({instance_url})..."
            )
            packages = fetcher.fetch()
            dataset_count = packages.num_rows
            logger.info(f"Found {dataset_count} packages for {instance_name}")
            dataset_result = ingestor.ingest_dataset(packages)

            # 2. Extract and ingest resources
            logger.info(f"Ingesting resources for {instance_name}...")
            resources_col = packages["resources"].combine_chunks().flatten()
            resources = pyarrow.Table.from_struct_array(resources_col)
            # Tag every resource with its instance URL so enqueue_outdated
            # can filter by instance.
            ckan_col = pyarrow.array(
                [instance_url] * resources.num_rows, type=pyarrow.string()
            )
            if "ckan_url" in resources.column_names:
                idx = resources.schema.get_field_index("ckan_url")
                resources = resources.set_column(
                    idx, pyarrow.field("ckan_url", pyarrow.string()), ckan_col
                )
            else:
                resources = resources.append_column(
                    pyarrow.field("ckan_url", pyarrow.string()), ckan_col
                )
            resource_count = resources.num_rows
            resource_result = ingestor.ingest_resources(resources)

            # 3. A dataset is considered updated when it changed directly OR any
            #    of its resources was updated.
            updated_dataset_ids = set(dataset_result.updated_ids)
            updated_resource_ids = resource_result.updated_ids
            if updated_resource_ids:
                placeholders = ",".join(["?"] * len(updated_resource_ids))
                rows = conn.execute(
                    f"SELECT DISTINCT package_id FROM ckan_resource "
                    f"WHERE id IN ({placeholders})",
                    updated_resource_ids,
                ).fetchall()
                updated_dataset_ids.update(row[0] for row in rows)

            logger.info(
                f"Sync complete for {instance_name}: {dataset_count} datasets, {resource_count} resources"
            )

            return {
                "instance_id": instance_id,
                "instance_name": instance_name,
                "total_packages": dataset_count,
                "new_datasets": dataset_result.new,
                "new_resources": resource_result.new,
                "updated_datasets": len(updated_dataset_ids),
                "updated_resources": resource_result.updated,
                "dataset_count": dataset_count,
                "resource_count": resource_count,
            }
        finally:
            conn.close()
