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
"""Tests for the syncs entity: recording sync runs, counts, and the syncs API."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pyarrow as pa
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestor_orchestrator.models import CkanInstance, MetadataSync
from ingestor_orchestrator.services.sync_service import SyncService


@pytest_asyncio.fixture
async def sess(engine, _create_tables):
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def instance(sess):
    inst = CkanInstance(
        id="inst-sync",
        name="Default",
        url="https://dados.pbh.gov.br",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    sess.add(inst)
    await sess.commit()
    return inst


def _dataset(ds_id, modified):
    return {"id": ds_id, "name": ds_id, "metadata_modified": modified}


def _resource(res_id, ds_id, modified):
    return {
        "id": res_id,
        "name": res_id,
        "url": f"http://example.com/{res_id}",
        "format": "CSV",
        "last_modified": modified,
        "package_id": ds_id,
        "datastore_active": True,
    }


def _packages_table(datasets, resources_by_dataset):
    rows = []
    for ds in datasets:
        row = dict(ds)
        row["resources"] = resources_by_dataset.get(ds["id"], [])
        rows.append(row)
    return pa.Table.from_pylist(rows)


class TestSyncService:
    pytestmark = pytest.mark.asyncio

    async def test_start_sync_creates_record(self, sess):
        record = await SyncService(sess).start_sync("inst-1")

        assert record.id
        assert record.instance_id == "inst-1"
        assert record.start_time is not None
        assert record.end_time is None

    async def test_finish_sync_updates_counts_and_end_time(self, sess):
        service = SyncService(sess)
        record = await service.start_sync("inst-1")

        finished = await service.finish_sync(
            record,
            {
                "total_packages": 10,
                "new_datasets": 2,
                "new_resources": 3,
                "updated_datasets": 1,
                "updated_resources": 4,
            },
        )

        assert finished.end_time is not None
        assert finished.total_packages == 10
        assert finished.new_datasets == 2
        assert finished.new_resources == 3
        assert finished.updated_datasets == 1
        assert finished.updated_resources == 4

    async def test_metadata_sync_dispatches_to_thread(self):
        """SyncService.sync_metadata_for_instance must run via asyncio.to_thread."""
        service = SyncService(db=AsyncMock())

        with (
            patch.object(SyncService, "_run_metadata_sync") as mock_run,
            patch.object(asyncio, "to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            mock_to_thread.return_value = {"dataset_count": 2, "resource_count": 5}
            result = await service.sync_metadata_for_instance(
                "inst-1", "Test", "https://test.example.com"
            )

        mock_to_thread.assert_awaited_once()
        # The blocking sync must NOT run directly on the event loop
        mock_run.assert_not_called()
        assert result["dataset_count"] == 2


class TestSyncsApi:
    pytestmark = pytest.mark.asyncio

    async def test_list_syncs_returns_records_with_instance_name(self, sess, instance):
        from ingestor_orchestrator.api.syncs import list_syncs

        service = SyncService(sess)
        record = await service.start_sync(instance.id)
        await service.finish_sync(
            record,
            {
                "total_packages": 5,
                "new_datasets": 1,
                "new_resources": 2,
                "updated_datasets": 0,
                "updated_resources": 1,
            },
        )

        result = await list_syncs(limit=50, offset=0, db=sess)

        assert len(result) == 1
        sync = result[0]
        assert sync.instance_id == instance.id
        assert sync.instance_name == "Default"
        assert sync.total_packages == 5
        assert sync.new_datasets == 1
        assert sync.new_resources == 2
        assert sync.updated_datasets == 0
        assert sync.updated_resources == 1
        assert sync.end_time is not None

    async def test_sync_failure_still_sets_end_time(self, sess, instance):
        """When sync raises, finish_sync must still be called so end_time is set."""
        from unittest.mock import patch

        from ingestor_orchestrator.api.metadata import sync_instance
        from sqlalchemy import select

        service = SyncService(sess)

        # Simulate a failure during sync_metadata_for_instance
        with patch.object(
            service,
            "sync_metadata_for_instance",
            side_effect=RuntimeError("boom"),
        ):
            # Let start_sync create a real record, but return our service
            # with the patched sync_metadata_for_instance
            with patch(
                "ingestor_orchestrator.api.metadata.SyncService",
                return_value=service,
            ):
                result = await sync_instance(instance.id, db=sess)

        assert "error" in result

        # The record created by start_sync must have end_time and status set
        record = (
            (await sess.execute(select(MetadataSync).limit(1)))
            .scalars()
            .one()
        )
        assert record.end_time is not None
        assert record.status == "failure"

    async def test_successful_sync_sets_status_success(self, sess, instance):
        """A successful sync must have status='success'."""
        service = SyncService(sess)
        record = await service.start_sync(instance.id)

        finished = await service.finish_sync(
            record, {"total_packages": 1}, status="success"
        )

        assert finished.status == "success"
        assert finished.end_time is not None


class TestSyncMetadataCounts:
    """sync_metadata_for_instance must report new/updated dataset and resource counts."""

    def _run_sync(self, packages, db_path, instance_url="https://test.example.com"):
        import duckdb

        class FakeFetcher:
            def __init__(self, url):
                pass

            def fetch(self):
                return packages

        def _make_conn(settings):
            return duckdb.connect(db_path)

        with (
            patch(
                "ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher",
                FakeFetcher,
            ),
            patch(
                "ckan_ingestor.duckdb_connection_factory.from_settings",
                side_effect=_make_conn,
            ),
        ):
            return SyncService._run_metadata_sync("inst-1", "Test", instance_url)

    def test_first_sync_counts_all_as_new(self, tmp_path):
        packages = _packages_table(
            [_dataset("ds-1", "2024-01-01"), _dataset("ds-2", "2024-01-01")],
            {
                "ds-1": [_resource("res-1", "ds-1", "2024-01-01")],
                "ds-2": [_resource("res-2", "ds-2", "2024-01-01")],
            },
        )
        result = self._run_sync(packages, str(tmp_path / "test.duckdb"))

        assert result["total_packages"] == 2
        assert result["new_datasets"] == 2
        assert result["new_resources"] == 2
        assert result["updated_datasets"] == 0
        assert result["updated_resources"] == 0

    def test_dataset_and_resource_update_counts(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        initial = _packages_table(
            [_dataset("ds-1", "2024-01-01")],
            {"ds-1": [_resource("res-1", "ds-1", "2024-01-01")]},
        )
        updated = _packages_table(
            [_dataset("ds-1", "2025-01-01")],
            {"ds-1": [_resource("res-1", "ds-1", "2025-01-01")]},
        )
        self._run_sync(initial, db_path)
        result = self._run_sync(updated, db_path)

        assert result["new_datasets"] == 0
        assert result["new_resources"] == 0
        assert result["updated_datasets"] == 1
        assert result["updated_resources"] == 1

    def test_resource_update_marks_dataset_as_updated(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        initial = _packages_table(
            [_dataset("ds-1", "2024-01-01")],
            {"ds-1": [_resource("res-1", "ds-1", "2024-01-01")]},
        )
        updated = _packages_table(
            [_dataset("ds-1", "2024-01-01")],
            {"ds-1": [_resource("res-1", "ds-1", "2025-01-01")]},
        )
        self._run_sync(initial, db_path)
        result = self._run_sync(updated, db_path)

        assert result["new_datasets"] == 0
        assert result["new_resources"] == 0
        assert result["updated_resources"] == 1
        assert result["updated_datasets"] == 1

    def test_mixed_new_and_updated(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        initial = _packages_table(
            [_dataset("ds-1", "2024-01-01")],
            {"ds-1": [_resource("res-1", "ds-1", "2024-01-01")]},
        )
        updated = _packages_table(
            [_dataset("ds-1", "2025-01-01"), _dataset("ds-2", "2025-01-01")],
            {
                "ds-1": [_resource("res-1", "ds-1", "2025-01-01")],
                "ds-2": [_resource("res-2", "ds-2", "2025-01-01")],
            },
        )
        self._run_sync(initial, db_path)
        result = self._run_sync(updated, db_path)

        assert result["new_datasets"] == 1
        assert result["new_resources"] == 1
        assert result["updated_datasets"] == 1
        assert result["updated_resources"] == 1
