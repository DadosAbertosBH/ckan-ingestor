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
"""Tests for the JobRepository — verifies SQL queries don't load heavy columns."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import event

from ingestor_orchestrator.models import (
    CkanDataJob,
    CkanDataJobResult,
    CkanInstance,
    JobStatus,
)
from ingestor_orchestrator.repositories.sqlalchemy_job_repository import (
    SqlAlchemyJobRepository,
)

pytestmark = pytest.mark.asyncio


async def _create_job_with_result(sess, instance: CkanInstance, resource_id: str):
    job = CkanDataJob(
        resource_id=resource_id,
        resource_name="Test Resource",
        resource_url=None,
        resource_format="CSV",
        dataset_name="ds-test",
        status=JobStatus.COMPLETED,
        idempotency_key=resource_id,
        instance_id=instance.id,
        ckan_url=instance.url,
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
    return job


class TestJobRepositoryListJobsDoesNotLoadDatasetPreview:
    """list_jobs must NOT trigger any query that selects dataset_preview.

    dataset_preview can be up to 20MB. Loading it in list queries causes
    MySQL "Out of sort memory" (error 1038) when the selectinload on
    results triggers an ORDER BY created_at filesort.
    """

    async def test_list_jobs_sql_excludes_dataset_preview(
        self, db_session, default_instance
    ):
        """RED: list_jobs must not load dataset_preview."""
        await _create_job_with_result(db_session, default_instance, "r-sql-preview")

        repo = SqlAlchemyJobRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            jobs, _ = await repo.list_jobs(limit=50, offset=0)

            assert len(jobs) >= 1

            # Check ALL captured SQL — none should include dataset_preview
            for sql in captured_sql:
                assert "dataset_preview" not in sql, (
                    f"list_jobs must NOT select dataset_preview. Captured SQL: {sql}"
                )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)

    async def test_list_jobs_lazy_loading_results(self, db_session, default_instance):
        """GREEN: list_jobs must NOT eagerly load results at all."""
        await _create_job_with_result(db_session, default_instance, "r-lazy")

        repo = SqlAlchemyJobRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            jobs, _ = await repo.list_jobs(limit=50, offset=0)

            # Check that no query against ckan_data_job_result was emitted
            result_queries = [
                sql for sql in captured_sql if "ckan_data_job_result" in sql
            ]
            assert len(result_queries) == 0, (
                f"list_jobs must not query ckan_data_job_result. "
                f"Captured: {result_queries}"
            )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)

    async def test_get_job_eagerly_loads_results(self, db_session, default_instance):
        """get_job must load results via selectinload (for the detail page)."""
        job = await _create_job_with_result(db_session, default_instance, "r-detail")

        repo = SqlAlchemyJobRepository(db_session)

        captured_sql: list[str] = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            captured_sql.append(str(statement))

        event.listen(db_session.bind.sync_engine, "before_cursor_execute", capture)

        try:
            result = await repo.get_job(job.id)

            assert result is not None
            # Results must be accessible (eagerly loaded)
            assert len(result.results) == 1
            assert result.results[0].dataset_preview == [{"col": "val"}]

            # Verify results were loaded via selectinload (second query exists)
            result_queries = [
                sql for sql in captured_sql if "ckan_data_job_result" in sql
            ]
            assert len(result_queries) == 1, (
                "get_job must use selectinload to load results"
            )
        finally:
            event.remove(db_session.bind.sync_engine, "before_cursor_execute", capture)
