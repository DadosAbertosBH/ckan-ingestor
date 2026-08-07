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
import datetime

from ingestor_orchestrator.models import CkanDataJob, JobStatus
from ingestor_orchestrator.services.job_service import JobService


def _make_success_result_data(preview=None):
    """Build result_data dict matching what the Rust consumer publishes."""
    data = {
        "job_id": "dummy",
        "status": "success",
        "rows_processed": 42,
        "labels": [],
        "datastore_active": False,
    }
    if preview is not None:
        data["preview"] = preview
    return data


class TestApplyResultPreview:
    async def test_preview_is_none_when_missing(self, db_session, default_instance):
        """RED: dataset_preview must be None when result_data has no 'preview' key."""
        job = CkanDataJob(
            resource_id="r-1",
            dataset_name="ds-1",
            idempotency_key="r-1",
            instance_id=default_instance.id,
            status=JobStatus.PENDING,
        )
        db_session.add(job)
        await db_session.flush()

        data = _make_success_result_data()
        data["job_id"] = job.id

        service = JobService(db_session)
        await service.apply_result(data)

        await db_session.refresh(job)
        assert len(job.results) == 1
        assert job.results[0].dataset_preview is None

    async def test_preview_is_populated_from_result_data(
        self, db_session, default_instance
    ):
        """RED: dataset_preview must be populated when result_data has 'preview'."""
        job = CkanDataJob(
            resource_id="r-2",
            dataset_name="ds-2",
            idempotency_key="r-2",
            instance_id=default_instance.id,
            status=JobStatus.PENDING,
        )
        db_session.add(job)
        await db_session.flush()

        preview = [{"col_a": "val1", "col_b": 42}, {"col_a": "val2", "col_b": 99}]
        data = _make_success_result_data(preview=preview)
        data["job_id"] = job.id

        service = JobService(db_session)
        await service.apply_result(data)

        await db_session.refresh(job)
        assert len(job.results) == 1
        assert job.results[0].dataset_preview == preview

    async def test_preview_datetime_is_sanitized(self, db_session, default_instance):
        """RED: dataset_preview with datetime values must be JSON-serializable."""
        job = CkanDataJob(
            resource_id="r-3",
            dataset_name="ds-3",
            idempotency_key="r-3",
            instance_id=default_instance.id,
            status=JobStatus.PENDING,
        )
        db_session.add(job)
        await db_session.flush()

        preview = [
            {"col": datetime.datetime(2026, 1, 1, 12, 0, 0)},
            {"col": datetime.date(2026, 6, 15)},
        ]
        data = _make_success_result_data(preview=preview)
        data["job_id"] = job.id

        service = JobService(db_session)
        await service.apply_result(data)

        await db_session.refresh(job)
        stored = job.results[0].dataset_preview
        assert stored is not None
        # Should be serializable (no TypeError on json.dumps)
        import json

        json.dumps(stored)
