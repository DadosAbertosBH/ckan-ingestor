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
"""Tests for enqueue_outdated_resources — sync endpoint must create jobs."""

from datetime import datetime, timezone

import duckdb
import pytest
import pytest_asyncio
from ingestor_orchestrator.models import CkanDataJob, CkanInstance
from ingestor_orchestrator.services.metadata_sync import enqueue_outdated_resources
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = pytest.mark.asyncio


@pytest.fixture
def duck_conn():
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE ckan_dataset (
            id VARCHAR, name VARCHAR, metadata_modified VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE ckan_resource (
            id VARCHAR, name VARCHAR, url VARCHAR, format VARCHAR,
            package_id VARCHAR, last_modified VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE ckan_resource_last_update (
            ckan_resource_id VARCHAR, last_modified TIMESTAMP
        )
    """)
    yield con
    con.close()


@pytest_asyncio.fixture
async def instance_id(engine, _create_tables):
    """Create an instance using autocommit session."""
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        inst = CkanInstance(
            id="inst-enqueue-test",
            name="Enqueue Test",
            url="https://test.example.com",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(inst)
        await session.commit()
        return inst.id


@pytest_asyncio.fixture
async def autocommit_session(engine, _create_tables):
    """Session without outer transaction — allows internal commits."""
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()


class TestEnqueueOutdatedResources:
    async def test_enqueues_jobs_for_outdated_resources(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """Resources not in ckan_resource_last_update get jobs created."""
        duck_conn.execute("INSERT INTO ckan_dataset VALUES ('ds1', 'D1', '2025-01-01')")
        duck_conn.execute(
            "INSERT INTO ckan_resource VALUES "
            "('r1', 'R1', 'http://a', 'CSV', 'ds1', '2025-01-01'),"
            "('r2', 'R2', 'http://b', 'JSON', 'ds1', '2025-01-01')"
        )

        monkeypatch.setattr(
            "ckan_ingestor.duckdb_connection_factory.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", db=autocommit_session
        )

        assert count == 2
        jobs = (await autocommit_session.execute(select(CkanDataJob))).scalars().all()
        assert len(jobs) == 2
        assert all(j.instance_id == instance_id for j in jobs)
        assert {j.resource_id for j in jobs} == {"r1", "r2"}

    async def test_skips_already_synced_resources(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """Resources with newer last_modified in last_update are skipped."""
        duck_conn.execute("INSERT INTO ckan_dataset VALUES ('ds1', 'D1', '2025-01-01')")
        duck_conn.execute(
            "INSERT INTO ckan_resource VALUES "
            "('r1', 'R1', 'http://a', 'CSV', 'ds1', '2025-01-01')"
        )
        duck_conn.execute(
            "INSERT INTO ckan_resource_last_update VALUES ('r1', '2025-06-01')"
        )

        monkeypatch.setattr(
            "ckan_ingestor.duckdb_connection_factory.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", db=autocommit_session
        )

        assert count == 0

    async def test_returns_zero_for_empty_db(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """Empty DuckDB returns 0 jobs."""
        monkeypatch.setattr(
            "ckan_ingestor.duckdb_connection_factory.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", db=autocommit_session
        )

        assert count == 0
