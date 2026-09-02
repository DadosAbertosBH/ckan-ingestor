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
"""Tests for the syncs entity: recording sync runs, counts, and the syncs API."""

from datetime import datetime, timezone
import json
import logging
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

    async def test_failed_sync_logs_worker_error(self, sess, caplog):
        record = await SyncService(sess).start_sync("inst-1")

        with caplog.at_level(logging.ERROR):
            await SyncService(sess).finish_sync(
                record,
                {"error_message": "HTTP 403 Forbidden from CKAN"},
                status="failure",
            )

        assert "status=failure" in caplog.text
        assert "HTTP 403 Forbidden from CKAN" in caplog.text
        assert record.error_message == "HTTP 403 Forbidden from CKAN"

    async def test_metadata_sync_publishes_an_async_worker_command(self, sess, instance):
        service = SyncService(sess)

        record = await service.sync_metadata_for_instance(
            instance.id, instance.name, instance.url
        )

        assert record.status == "pending"
        from ingestor_orchestrator.iggy_queue import get_iggy_bus

        payload = get_iggy_bus().publish.await_args.args[1]
        assert get_iggy_bus().publish.await_args.args[0] == "ckan_metadata_sync"
        assert get_iggy_bus().publish.await_args.kwargs["key"] == record.id
        assert json.loads(payload) == {
            "sync_id": record.id,
            "instance_id": instance.id,
            "instance_name": instance.name,
            "instance_url": instance.url,
        }


class TestSyncsApi:
    pytestmark = pytest.mark.asyncio

    async def test_list_syncs_returns_records_with_instance_name(self, sess, instance):
        from ingestor_orchestrator.api.syncs import list_syncs
        from ingestor_orchestrator.repositories import SqlAlchemySyncRepository

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

        result = await list_syncs(
            limit=50, offset=0, repo=SqlAlchemySyncRepository(sess)
        )

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

    async def test_list_syncs_exposes_sync_failure_error(self, sess, instance):
        from ingestor_orchestrator.api.syncs import list_syncs
        from ingestor_orchestrator.repositories import SqlAlchemySyncRepository

        service = SyncService(sess)
        record = await service.start_sync(instance.id)
        await service.finish_sync(
            record,
            {"error_message": "HTTP 403 Forbidden from CKAN"},
            status="failure",
        )

        sync = (
            await list_syncs(
                limit=50, offset=0, repo=SqlAlchemySyncRepository(sess)
            )
        )[0]

        assert sync.status == "failure"
        assert sync.error_message == "HTTP 403 Forbidden from CKAN"

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
        record = (await sess.execute(select(MetadataSync).limit(1))).scalars().one()
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
