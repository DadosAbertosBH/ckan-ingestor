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
"""Tests for CkanDataJob.status enum — must use values (lowercase), not names (uppercase)."""

import pytest
from ingestor_orchestrator.models import CkanDataJob, JobStatus
from sqlalchemy import Enum as SAEnum


class TestJobStatusEnum:
    def test_enum_values_are_lowercase(self):
        """The Python enum values must be lowercase strings."""
        assert JobStatus.PENDING.value == "pending"
        assert JobStatus.PROCESSING.value == "processing"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.FAILED.value == "failed"

    @pytest.mark.asyncio
    async def test_sa_enum_uses_values_not_names(self, db_session):
        """SAEnum must use .value (lowercase), not .name (UPPERCASE)."""
        sa_enum = CkanDataJob.__table__.c.status.type
        assert isinstance(sa_enum, SAEnum)
        assert sa_enum.enums == [
            "pending",
            "processing",
            "completed",
            "failed",
            "deleted",
        ]

    @pytest.mark.asyncio
    async def test_job_persists_with_enum_value(self, db_session, default_instance):
        """Creating a CkanDataJob with a status value persists correctly."""
        job = CkanDataJob(
            resource_id="r-enum-test",
            dataset_name="d-enum-test",
            idempotency_key="ik-enum-test",
            instance_id=default_instance.id,
            status=JobStatus.PENDING,
        )
        db_session.add(job)
        await db_session.flush()

        assert job.status == JobStatus.PENDING
        assert job.status.value == "pending"

    @pytest.mark.asyncio
    async def test_all_enum_values_persist(self, db_session, default_instance):
        """All four status enum values can be persisted."""
        for i, status in enumerate(JobStatus):
            job = CkanDataJob(
                resource_id=f"r-enum-{i}",
                dataset_name="d-enum",
                idempotency_key=f"ik-enum-{i}",
                instance_id=default_instance.id,
                status=status,
            )
            db_session.add(job)
        await db_session.flush()
