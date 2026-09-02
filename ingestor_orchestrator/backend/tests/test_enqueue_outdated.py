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
"""Tests for enqueue_outdated_resources — sync endpoint must create jobs."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import duckdb
import pytest
import pytest_asyncio
from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanInstance,
    JobStatus,
    LatestResourceJob,
)
from ingestor_orchestrator.services.job_service import JobService
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
            package_id VARCHAR, last_modified VARCHAR, ckan_url VARCHAR,
            datastore_active BOOLEAN DEFAULT false
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
    async def test_failed_resource_is_published_to_retry_topic(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """An automatically re-enqueued failed resource uses the retry topic."""
        duck_conn.execute("INSERT INTO ckan_dataset VALUES ('ds1', 'D1', '2025-01-01')")
        duck_conn.execute(
            "INSERT INTO ckan_resource VALUES "
            "('r1', 'R1', 'http://a', 'CSV', 'ds1', '2025-01-01', '', false)"
        )
        failed = CkanDataJob(
            resource_id="r1",
            dataset_name="D1",
            idempotency_key="r1",
            instance_id=instance_id,
            status=JobStatus.FAILED,
        )
        autocommit_session.add(failed)
        await autocommit_session.flush()
        autocommit_session.add(
            LatestResourceJob(
                resource_id="r1",
                latest_job_id=failed.id,
                instance_id=instance_id,
                dataset_name="D1",
                status=JobStatus.FAILED,
            )
        )
        await autocommit_session.commit()

        publish = AsyncMock(
            return_value=MagicMock(
                broker_type="iggy",
                stream="ckan-ingestor",
                topic="jobs-retry",
                partition=0,
                offset=None,
            )
        )
        monkeypatch.setattr(JobService, "_publish_job", publish)
        monkeypatch.setattr(
            "ingestor_orchestrator.ducklake.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", db=autocommit_session
        )

        assert count == 1
        assert publish.await_args.kwargs["retry"] is True

    async def test_enqueues_jobs_for_outdated_resources(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """Resources not in ckan_resource_last_update get jobs created."""
        duck_conn.execute("INSERT INTO ckan_dataset VALUES ('ds1', 'D1', '2025-01-01')")
        duck_conn.execute(
            "INSERT INTO ckan_resource VALUES "
            "('r1', 'R1', 'http://a', 'CSV', 'ds1', '2025-01-01', '', true),"
            "('r2', 'R2', 'http://b', 'JSON', 'ds1', '2025-01-01', '', false)"
        )

        monkeypatch.setattr(
            "ingestor_orchestrator.ducklake.from_settings",
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
        assert {j.resource_id: j.datastore_active for j in jobs} == {
            "r1": True,
            "r2": False,
        }

    async def test_enqueues_resources_with_duckdb_uuid_ids_as_strings(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        resource_id = UUID("d3260a73-c176-44c7-9e9a-fdf2e1b91ab1")
        dataset_id = UUID("d3260a73-c176-44c7-9e9a-fdf2e1b91ab2")
        duck_conn.execute("DROP TABLE ckan_resource")
        duck_conn.execute("DROP TABLE ckan_dataset")
        duck_conn.execute(
            "CREATE TABLE ckan_dataset "
            "(id UUID, name VARCHAR, metadata_modified VARCHAR)"
        )
        duck_conn.execute(
            "CREATE TABLE ckan_resource ("
            "id UUID, name VARCHAR, url VARCHAR, format VARCHAR, "
            "package_id UUID, last_modified VARCHAR, ckan_url VARCHAR, "
            "datastore_active BOOLEAN)"
        )
        duck_conn.execute(
            "INSERT INTO ckan_dataset VALUES (?, 'D1', '2025-01-01')",
            [dataset_id],
        )
        duck_conn.execute(
            "INSERT INTO ckan_resource VALUES (?, 'R1', 'http://a', 'CSV', "
            "?, '2025-01-01', '', false)",
            [resource_id, dataset_id],
        )
        monkeypatch.setattr(
            "ingestor_orchestrator.ducklake.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", db=autocommit_session
        )

        assert count == 1
        job = (await autocommit_session.execute(select(CkanDataJob))).scalar_one()
        assert job.resource_id == str(resource_id)

    async def test_skips_already_synced_resources(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """Resources with newer last_modified in last_update are skipped."""
        duck_conn.execute("INSERT INTO ckan_dataset VALUES ('ds1', 'D1', '2025-01-01')")
        duck_conn.execute(
            "INSERT INTO ckan_resource (id, name, url, format, package_id, last_modified, ckan_url) VALUES "
            "('r1', 'R1', 'http://a', 'CSV', 'ds1', '2025-01-01', '')"
        )
        duck_conn.execute(
            "INSERT INTO ckan_resource_last_update VALUES ('r1', '2025-06-01')"
        )

        monkeypatch.setattr(
            "ingestor_orchestrator.ducklake.from_settings",
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
            "ingestor_orchestrator.ducklake.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", db=autocommit_session
        )

        assert count == 0

    async def test_filters_by_ckan_url(
        self, autocommit_session, instance_id, duck_conn, monkeypatch
    ):
        """Only resources matching the instance's ckan_url are enqueued."""
        instance_url = "https://dados.pbh.gov.br"
        other_url = "https://dados.mg.gov.br"

        duck_conn.execute("INSERT INTO ckan_dataset VALUES ('ds1', 'D1', '2025-01-01')")
        duck_conn.execute(
            "INSERT INTO ckan_resource (id, name, url, format, package_id, last_modified, ckan_url) VALUES "
            "('r1', 'R1', 'http://a', 'CSV', 'ds1', '2025-01-01', '"
            + instance_url
            + "'),"
            "('r2', 'R2', 'http://b', 'JSON', 'ds1', '2025-01-01', '" + other_url + "')"
        )

        monkeypatch.setattr(
            "ingestor_orchestrator.ducklake.from_settings",
            lambda _: duck_conn,
        )

        count = await enqueue_outdated_resources(
            instance_id, "test", ckan_url=instance_url, db=autocommit_session
        )

        # Only r1 (matching instance_url) should be enqueued, not r2
        assert count == 1
        jobs = (await autocommit_session.execute(select(CkanDataJob))).scalars().all()
        assert len(jobs) == 1
        assert jobs[0].resource_id == "r1"
