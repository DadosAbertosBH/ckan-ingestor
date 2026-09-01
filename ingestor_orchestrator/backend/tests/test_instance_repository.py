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
"""Tests for the InstanceRepository."""

import pytest
from sqlalchemy import event

from ingestor_orchestrator.models import CkanInstance


pytestmark = pytest.mark.asyncio


class TestListInstances:
    async def test_returns_all_instances_ordered_by_name(
        self, db_session, default_instance
    ):
        """list_instances returns all instances ordered by name."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        # Create extra instances to verify ordering
        db_session.add(CkanInstance(name="Zulu", url="https://z.example.com"))
        db_session.add(CkanInstance(name="Alpha", url="https://a.example.com"))
        await db_session.flush()

        repo = SqlAlchemyInstanceRepository(db_session)
        instances = await repo.list_instances()

        names = [i.name for i in instances]
        assert names == sorted(names), f"Expected alphabetical order, got {names}"

    async def test_returns_empty_list_when_no_instances(self, db_session):
        """list_instances returns empty list when there are no instances."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        repo = SqlAlchemyInstanceRepository(db_session)
        instances = await repo.list_instances()

        assert instances == []


class TestGetInstance:
    async def test_returns_instance_by_id(self, db_session, default_instance):
        """get_instance returns the matching instance."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        repo = SqlAlchemyInstanceRepository(db_session)
        instance = await repo.get_instance(default_instance.id)

        assert instance is not None
        assert instance.id == default_instance.id
        assert instance.name == "Default"

    async def test_returns_none_when_not_found(self, db_session):
        """get_instance returns None for a non-existent ID."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        repo = SqlAlchemyInstanceRepository(db_session)
        instance = await repo.get_instance("nonexistent-id")

        assert instance is None


class TestCreateInstance:
    async def test_persists_and_returns_instance(self, db_session):
        """create_instance persists the instance and returns it with an ID."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        instance = CkanInstance(name="New Instance", url="https://new.example.com")
        repo = SqlAlchemyInstanceRepository(db_session)

        result = await repo.create_instance(instance)

        assert result.id is not None
        assert result.name == "New Instance"
        assert result.url == "https://new.example.com"

        # Verify it was persisted
        fetched = await repo.get_instance(result.id)
        assert fetched is not None
        assert fetched.name == "New Instance"


class TestDeleteInstance:
    async def test_deletes_and_returns_true(self, db_session, default_instance):
        """delete_instance deletes the instance and returns True."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        repo = SqlAlchemyInstanceRepository(db_session)

        result = await repo.delete_instance(default_instance.id)

        assert result is True

        # Verify it's gone
        fetched = await repo.get_instance(default_instance.id)
        assert fetched is None

    async def test_returns_false_when_not_found(self, db_session):
        """delete_instance returns False for a non-existent ID."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        repo = SqlAlchemyInstanceRepository(db_session)

        result = await repo.delete_instance("nonexistent-id")

        assert result is False


class TestGetInstancesByIds:
    async def test_returns_instances_for_given_ids(self, db_session, default_instance):
        """get_instances_by_ids returns only matching instances."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        inst2 = CkanInstance(name="Second", url="https://s.example.com")
        inst3 = CkanInstance(name="Third", url="https://t.example.com")
        db_session.add_all([inst2, inst3])
        await db_session.flush()

        repo = SqlAlchemyInstanceRepository(db_session)

        result = await repo.get_instances_by_ids([default_instance.id, inst2.id])

        ids = {i.id for i in result}
        assert default_instance.id in ids
        assert inst2.id in ids
        assert inst3.id not in ids

    async def test_returns_empty_list_for_no_matching_ids(self, db_session):
        """get_instances_by_ids returns empty list when none match."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        repo = SqlAlchemyInstanceRepository(db_session)

        result = await repo.get_instances_by_ids(["nonexistent-1", "nonexistent-2"])

        assert result == []

    async def test_uses_single_query(self, db_session, default_instance):
        """get_instances_by_ids emits exactly one SQL query."""
        from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
            SqlAlchemyInstanceRepository,
        )

        inst2 = CkanInstance(name="Second", url="https://s.example.com")
        inst3 = CkanInstance(name="Third", url="https://t.example.com")
        db_session.add_all([inst2, inst3])
        await db_session.flush()

        repo = SqlAlchemyInstanceRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            await repo.get_instances_by_ids([default_instance.id, inst2.id])

            select_queries = [
                sql
                for sql in captured_sql
                if sql.strip().upper().startswith("SELECT") and "ckan_instance" in sql
            ]
            assert len(select_queries) == 1, (
                f"Expected 1 SELECT query, got {len(select_queries)}: {select_queries}"
            )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)
