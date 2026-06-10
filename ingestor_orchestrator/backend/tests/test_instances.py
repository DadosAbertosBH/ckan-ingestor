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
"""Tests for CkanInstance model and instances API."""

import pytest
from ingestor_orchestrator.models import CkanInstance
from ingestor_orchestrator.schemas import CkanInstanceResponse
from sqlalchemy import select

pytestmark = pytest.mark.asyncio


class TestCkanInstanceModel:
    async def test_create_instance(self, db_session):
        """A CkanInstance can be created and persisted."""
        inst = CkanInstance(
            name="Test Instance",
            url="https://test.example.com",
        )
        db_session.add(inst)
        await db_session.flush()

        assert inst.id is not None
        assert inst.name == "Test Instance"
        assert inst.url == "https://test.example.com"
        assert inst.dataset_count == 0
        assert inst.resource_count == 0
        assert inst.last_metadata_synced is None
        assert inst.created_at is not None
        assert inst.updated_at is not None

    async def test_name_unique_constraint(self, db_session, default_instance):
        """Instance name must be unique."""
        inst = CkanInstance(
            name=default_instance.name,  # same name as default
            url="https://other.example.com",
        )
        db_session.add(inst)
        with pytest.raises(Exception):
            await db_session.flush()

    async def test_list_all_instances(self, db_session, default_instance):
        """Can list all instances."""
        inst2 = CkanInstance(
            name="Second",
            url="https://second.example.com",
        )
        db_session.add(inst2)
        await db_session.flush()

        result = await db_session.execute(select(CkanInstance))
        instances = result.scalars().all()
        assert len(instances) == 2

    async def test_get_instance_by_id(self, db_session, default_instance):
        """Can fetch an instance by ID."""
        inst = await db_session.get(CkanInstance, default_instance.id)
        assert inst is not None
        assert inst.name == "Default"

    async def test_to_response_schema(self, db_session, default_instance):
        """CkanInstance can be converted to CkanInstanceResponse."""
        resp = CkanInstanceResponse.model_validate(default_instance)
        assert resp.id == default_instance.id
        assert resp.name == "Default"
        assert resp.url == "https://dados.pbh.gov.br"
        assert resp.dataset_count == 0
        assert resp.resource_count == 0

    async def test_update_metadata_sync_timestamp(self, db_session, default_instance):
        """last_metadata_synced can be updated."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        default_instance.last_metadata_synced = now
        default_instance.dataset_count = 42
        default_instance.resource_count = 100
        await db_session.flush()

        refreshed = await db_session.get(CkanInstance, default_instance.id)
        assert refreshed.dataset_count == 42
        assert refreshed.resource_count == 100
        assert refreshed.last_metadata_synced is not None

    async def test_delete_instance_cascades_or_blocks(
        self, db_session, default_instance
    ):
        """Deleting an instance that has jobs should raise an error (FK constraint)."""

        from ingestor_orchestrator.models import CkanDataJob

        job = CkanDataJob(
            resource_id="r1",
            dataset_name="d1",
            idempotency_key="ik-delete-test",
            instance_id=default_instance.id,
        )
        db_session.add(job)
        await db_session.flush()

        # Attempting to delete the instance should fail due to FK
        await db_session.delete(default_instance)
        with pytest.raises(Exception):
            await db_session.flush()


class TestJobWithInstance:
    async def test_job_must_have_instance_id(self, db_session):
        """Creating a job without instance_id raises error."""
        from ingestor_orchestrator.models import CkanDataJob

        job = CkanDataJob(
            resource_id="r1",
            dataset_name="d1",
            idempotency_key="ik-no-instance",
        )
        db_session.add(job)
        with pytest.raises(Exception):
            await db_session.flush()

    async def test_job_with_instance(self, db_session, default_instance):
        """Job with valid instance_id can be created."""
        from ingestor_orchestrator.models import CkanDataJob

        job = CkanDataJob(
            resource_id="r1",
            dataset_name="d1",
            idempotency_key="ik-with-instance",
            instance_id=default_instance.id,
        )
        db_session.add(job)
        await db_session.flush()
        assert job.id is not None

    async def test_job_instance_relationship(self, db_session, default_instance):
        """Job.instance provides access to the CkanInstance."""
        from ingestor_orchestrator.models import CkanDataJob

        job = CkanDataJob(
            resource_id="r2",
            dataset_name="d2",
            idempotency_key="ik-rel-test",
            instance_id=default_instance.id,
        )
        db_session.add(job)
        await db_session.flush()

        # The relationship should work
        assert job.instance is not None
        assert job.instance.name == "Default"
        assert job.instance.url == "https://dados.pbh.gov.br"
