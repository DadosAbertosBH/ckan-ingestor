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
"""Tests for the ResourceRepository — verifies SQL queries and data access."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import event

from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanDataJobResult,
    CkanInstance,
    JobStatus,
    LastTerminalStatus,
    LatestResourceJob,
    ResourceMetadataLabel,
    ResourceStatus,
)
from ingestor_orchestrator.repositories.sqlalchemy_resource_repository import (
    SqlAlchemyResourceRepository,
)

pytestmark = pytest.mark.asyncio


async def _create_resource_with_job_and_result(
    sess, instance: CkanInstance, resource_id: str
):
    """Create a CkanDataJob with a result, a LatestResourceJob, and a label."""
    job = CkanDataJob(
        resource_id=resource_id,
        resource_name=f"Resource {resource_id}",
        resource_url=f"https://example.com/{resource_id}.csv",
        resource_format="CSV",
        dataset_name=f"ds-{resource_id}",
        status=JobStatus.COMPLETED,
        idempotency_key=resource_id,
        instance_id=instance.id,
        ckan_url=instance.url,
        created_at=datetime.now(timezone.utc),
    )
    sess.add(job)
    await sess.flush()

    sess.add(
        CkanDataJobResult(
            job_id=job.id,
            success=True,
            dataset_preview=[{"col": "val"}],
            rows_processed=1,
            created_at=datetime.now(timezone.utc),
        )
    )
    await sess.flush()

    latest = LatestResourceJob(
        resource_id=resource_id,
        latest_job_id=job.id,
        instance_id=instance.id,
        resource_name=f"Resource {resource_id}",
        resource_url=f"https://example.com/{resource_id}.csv",
        resource_format="CSV",
        dataset_name=f"ds-{resource_id}",
        status=JobStatus.COMPLETED,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    sess.add(latest)

    label = ResourceMetadataLabel(
        resource_id=resource_id,
        label="test-label",
        created_at=datetime.now(timezone.utc),
    )
    sess.add(label)

    await sess.flush()
    return latest


class TestResourceRepositoryListResourcesDoesNotLoadJobResults:
    """list_resources must eagerly load instance but NOT job results.

    CkanDataJobResult.dataset_preview can be up to 20MB. Loading it in
    list queries would cause unnecessary overhead and potential MySQL
    issues when sorting.
    """

    async def test_list_resources_sql_includes_instance_selectinload(
        self, db_session, default_instance
    ):
        """RED: list_resources must include selectinload for instance."""
        await _create_resource_with_job_and_result(
            db_session, default_instance, "r-sql-instance"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            resources, _, _ = await repo.list_resources(limit=50, offset=0)

            assert len(resources) >= 1

            # selectinload emits a separate SELECT against ckan_instance.
            # Verify that query was issued (first query is the main select,
            # second is the eager load of the instance relationship).
            instance_queries = [sql for sql in captured_sql if "ckan_instance" in sql]
            assert len(instance_queries) >= 1, (
                "list_resources must eagerly load instance via selectinload. "
                f"Captured SQL: {captured_sql}"
            )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)

    async def test_list_resources_sql_excludes_job_results(
        self, db_session, default_instance
    ):
        """RED: list_resources must NOT load ckan_data_job_result."""
        await _create_resource_with_job_and_result(
            db_session, default_instance, "r-sql-no-results"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            resources, _, _ = await repo.list_resources(limit=50, offset=0)

            assert len(resources) >= 1

            # No captured SQL should reference ckan_data_job_result
            result_queries = [
                sql for sql in captured_sql if "ckan_data_job_result" in sql
            ]
            assert len(result_queries) == 0, (
                "list_resources must not query ckan_data_job_result. "
                f"Captured: {result_queries}"
            )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)

    async def test_list_resources_returns_labels(self, db_session, default_instance):
        """list_resources must return labels mapped by resource_id."""
        await _create_resource_with_job_and_result(
            db_session, default_instance, "r-labels"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        resources, labels_map, _ = await repo.list_resources(limit=50, offset=0)

        assert len(resources) >= 1
        assert "r-labels" in labels_map
        assert labels_map["r-labels"] == ["test-label"]

    async def test_list_resources_returns_job_counts(
        self, db_session, default_instance
    ):
        """list_resources must return job counts mapped by resource_id."""
        await _create_resource_with_job_and_result(
            db_session, default_instance, "r-counts"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        resources, _, counts_map = await repo.list_resources(limit=50, offset=0)

        assert len(resources) >= 1
        assert "r-counts" in counts_map
        assert counts_map["r-counts"] == 1


class TestResourceRepositoryGetResource:
    """get_resource loads full detail including results on the latest job."""

    async def test_get_resource_eagerly_loads_latest_job_results(
        self, db_session, default_instance
    ):
        """get_resource must load results on the latest job via selectinload."""
        latest = await _create_resource_with_job_and_result(
            db_session, default_instance, "r-detail-results"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            detail = await repo.get_resource(latest.resource_id)

            assert detail is not None
            assert detail.latest_job is not None
            # Results must be accessible (eagerly loaded)
            assert len(detail.latest_job.results) == 1
            assert detail.latest_job.results[0].dataset_preview == [{"col": "val"}]

            # Verify results were loaded via selectinload
            result_queries = [
                sql for sql in captured_sql if "ckan_data_job_result" in sql
            ]
            assert len(result_queries) >= 1, (
                "get_resource must use selectinload to load results on latest_job"
            )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)

    async def test_get_resource_returns_all_jobs(self, db_session, default_instance):
        """get_resource must return all jobs for the resource."""
        latest = await _create_resource_with_job_and_result(
            db_session, default_instance, "r-all-jobs"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        detail = await repo.get_resource(latest.resource_id)

        assert detail is not None
        assert len(detail.all_jobs) == 1
        assert detail.all_jobs[0].resource_id == "r-all-jobs"

    async def test_get_resource_returns_labels(self, db_session, default_instance):
        """get_resource must return labels for the resource."""
        latest = await _create_resource_with_job_and_result(
            db_session, default_instance, "r-detail-labels"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        detail = await repo.get_resource(latest.resource_id)

        assert detail is not None
        assert detail.labels == ["test-label"]

    async def test_get_resource_with_resource(self, db_session, default_instance):
        """get_resource must return the LatestResourceJob with instance loaded."""
        latest = await _create_resource_with_job_and_result(
            db_session, default_instance, "r-full"
        )

        repo = SqlAlchemyResourceRepository(db_session)

        detail = await repo.get_resource(latest.resource_id)

        assert detail is not None
        assert detail.resource.resource_id == "r-full"
        assert detail.resource.resource_name == "Resource r-full"
        assert detail.resource.instance is not None
        assert detail.resource.instance.id == default_instance.id

    async def test_get_resource_nonexistent_returns_none(
        self, db_session, default_instance
    ):
        """get_resource returns None for a nonexistent resource_id."""
        repo = SqlAlchemyResourceRepository(db_session)
        detail = await repo.get_resource("nonexistent-id")
        assert detail is None

    async def test_list_resources_filters_outdated_status(
        self, db_session, default_instance
    ):
        """The resource status filter accepts outdated resources."""
        job = CkanDataJob(
            resource_id="r-outdated-filter",
            dataset_name="dataset",
            idempotency_key="r-outdated-filter",
            instance_id=default_instance.id,
            status=JobStatus.PENDING,
        )
        db_session.add(job)
        await db_session.flush()
        db_session.add(
            LatestResourceJob(
                resource_id=job.resource_id,
                latest_job_id=job.id,
                instance_id=default_instance.id,
                dataset_name=job.dataset_name,
                status=JobStatus.PENDING,
            )
        )
        db_session.add(
            LastTerminalStatus(
                resource_id=job.resource_id,
                last_terminal_job_id=job.id,
                last_terminal_status=JobStatus.COMPLETED,
                last_terminal_at=datetime.now(timezone.utc),
            )
        )
        await db_session.flush()

        resources, _, _ = await SqlAlchemyResourceRepository(db_session).list_resources(
            status=ResourceStatus.OUTDATED
        )

        assert [resource.resource_id for resource in resources] == [job.resource_id]
