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
"""Tests for SyncRepository — verifies sync list queries."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import event

from ingestor_orchestrator.models import CkanInstance, MetadataSync
from ingestor_orchestrator.repositories.sqlalchemy_sync_repository import (
    SqlAlchemySyncRepository,
)

pytestmark = pytest.mark.asyncio


async def _create_sync(
    sess, instance: CkanInstance, *, start_delta: int = 0, **kwargs
):
    """Helper to create a MetadataSync record."""
    sync = MetadataSync(
        instance_id=instance.id,
        start_time=datetime(2025, 1, 1, 12, 0, start_delta, tzinfo=timezone.utc),
        end_time=datetime(2025, 1, 1, 12, 5, start_delta, tzinfo=timezone.utc),
        status="completed",
        total_packages=100,
        new_datasets=5,
        new_resources=10,
        updated_datasets=3,
        updated_resources=7,
        **kwargs,
    )
    sess.add(sync)
    await sess.flush()
    return sync


class TestSyncRepository:
    async def test_list_syncs_returns_all_ordered_by_start_time_desc(
        self, db_session, default_instance
    ):
        """list_syncs returns all syncs ordered by start_time desc."""
        s1 = await _create_sync(db_session, default_instance, start_delta=0)
        s2 = await _create_sync(db_session, default_instance, start_delta=10)
        s3 = await _create_sync(db_session, default_instance, start_delta=20)

        repo = SqlAlchemySyncRepository(db_session)
        result = await repo.list_syncs()

        assert len(result) == 3
        # Most recent first
        assert result[0].id == s3.id
        assert result[1].id == s2.id
        assert result[2].id == s1.id

    async def test_list_syncs_filter_by_instance_id(
        self, db_session, default_instance
    ):
        """list_syncs filters by instance_id when provided."""
        other = CkanInstance(
            id="inst-other",
            name="Other",
            url="https://other.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(other)
        await db_session.flush()

        await _create_sync(db_session, default_instance, start_delta=0)
        await _create_sync(db_session, other, start_delta=10)

        repo = SqlAlchemySyncRepository(db_session)
        result = await repo.list_syncs(instance_id=default_instance.id)

        assert len(result) == 1
        assert result[0].instance_id == default_instance.id

    async def test_list_syncs_pagination(self, db_session, default_instance):
        """list_syncs respects limit and offset."""
        for i in range(5):
            await _create_sync(db_session, default_instance, start_delta=i)

        repo = SqlAlchemySyncRepository(db_session)

        page1 = await repo.list_syncs(limit=2, offset=0)
        assert len(page1) == 2

        page2 = await repo.list_syncs(limit=2, offset=2)
        assert len(page2) == 2

        page3 = await repo.list_syncs(limit=2, offset=4)
        assert len(page3) == 1

        # No overlap
        ids_page1 = {s.id for s in page1}
        ids_page2 = {s.id for s in page2}
        ids_page3 = {s.id for s in page3}
        assert ids_page1.isdisjoint(ids_page2)
        assert ids_page1.isdisjoint(ids_page3)
        assert ids_page2.isdisjoint(ids_page3)

    async def test_list_syncs_eagerly_loads_instance(
        self, db_session, default_instance
    ):
        """list_syncs must use selectinload on instance — verified via SQL capture."""
        await _create_sync(db_session, default_instance)

        repo = SqlAlchemySyncRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(
            db_session.bind.sync_engine, "before_cursor_execute", capture
        )

        try:
            result = await repo.list_syncs()

            assert len(result) == 1
            # Instance must be accessible (eagerly loaded)
            assert result[0].instance is not None
            assert result[0].instance.name == "Default"

            # There should be a second query loading ckan_instance
            instance_queries = [
                sql for sql in captured_sql if "ckan_instance" in sql
            ]
            assert len(instance_queries) >= 1, (
                "list_syncs must use selectinload to eagerly load instance"
            )
        finally:
            event.remove(
                db_session.bind.sync_engine, "before_cursor_execute", capture
            )

    async def test_list_syncs_empty_when_no_syncs(
        self, db_session, default_instance
    ):
        """list_syncs returns empty list when no syncs exist."""
        repo = SqlAlchemySyncRepository(db_session)
        result = await repo.list_syncs()
        assert result == []
