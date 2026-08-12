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
"""Tests for Pydantic schemas — labels, instances, JobCreate, and responses."""

from datetime import datetime, timezone

import pytest
from ingestor_orchestrator.dto import (
    CkanInstanceResponse,
    InstanceCreate,
    InstanceStats,
    JobCreate,
    JobListResponse,
    JobResponse,
)
from ingestor_orchestrator.models import JobStatus


def _make_datetime():
    return datetime.now(timezone.utc)


class TestCkanInstanceResponse:
    def test_from_attributes(self):
        """CkanInstanceResponse can be created from dict (ORM-compatible)."""
        now = _make_datetime()
        resp = CkanInstanceResponse(
            id="inst-1",
            name="PBH",
            url="https://dados.pbh.gov.br",
            last_metadata_synced=now,
            dataset_count=10,
            resource_count=50,
            created_at=now,
            updated_at=now,
        )
        assert resp.id == "inst-1"
        assert resp.name == "PBH"
        assert resp.url == "https://dados.pbh.gov.br"
        assert resp.dataset_count == 10
        assert resp.resource_count == 50

    def test_defaults(self):
        """Default values for optional fields."""
        now = _make_datetime()
        resp = CkanInstanceResponse(
            id="inst-1",
            name="Test",
            url="https://example.com",
            created_at=now,
            updated_at=now,
        )
        assert resp.last_metadata_synced is None
        assert resp.dataset_count == 0
        assert resp.resource_count == 0


class TestInstanceStats:
    def test_instance_stats_creation(self):
        """InstanceStats wraps CkanInstanceResponse with job counts."""
        now = _make_datetime()
        inst = CkanInstanceResponse(
            id="inst-1",
            name="PBH",
            url="https://dados.pbh.gov.br",
            created_at=now,
            updated_at=now,
        )
        stats = InstanceStats(
            instance=inst,
            pending=3,
            processing=1,
            completed=10,
            failed=2,
        )
        assert stats.instance.name == "PBH"
        assert stats.pending == 3
        assert stats.processing == 1
        assert stats.completed == 10
        assert stats.failed == 2

    def test_defaults_to_zero(self):
        """Job counts default to 0."""
        now = _make_datetime()
        inst = CkanInstanceResponse(
            id="inst-1",
            name="Empty",
            url="https://example.com",
            created_at=now,
            updated_at=now,
        )
        stats = InstanceStats(instance=inst)
        assert stats.pending == 0
        assert stats.processing == 0
        assert stats.completed == 0
        assert stats.failed == 0
        assert stats.empty == 0

    def test_empty_count_is_accepted(self):
        """InstanceStats accepts empty resource count."""
        now = _make_datetime()
        inst = CkanInstanceResponse(
            id="inst-1",
            name="Test",
            url="https://example.com",
            created_at=now,
            updated_at=now,
        )
        stats = InstanceStats(
            instance=inst,
            empty=5,
        )
        assert stats.empty == 5


class TestInstanceCreate:
    def test_required_fields(self):
        """name and url are required."""
        ic = InstanceCreate(name="PBH", url="https://dados.pbh.gov.br")
        assert ic.name == "PBH"
        assert ic.url == "https://dados.pbh.gov.br"

    def test_name_is_required(self):
        """name is required."""
        with pytest.raises(Exception):
            InstanceCreate(url="https://example.com")

    def test_url_is_required(self):
        """url is required."""
        with pytest.raises(Exception):
            InstanceCreate(name="Test")


class TestJobListResponse:
    def test_defaults_labels_to_empty_list(self):
        """labels field defaults to [] when not provided."""
        resp = JobListResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format="CSV",
            dataset_name="my-dataset",
            status=JobStatus.COMPLETED,
            idempotency_key="ik",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
        )
        assert resp.labels == []

    def test_labels_are_preserved(self):
        """labels list is preserved when explicitly provided."""
        resp = JobListResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format="CSV",
            dataset_name="my-dataset",
            status=JobStatus.COMPLETED,
            idempotency_key="ik",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
            labels=["empty"],
        )
        assert resp.labels == ["empty"]

    def test_labels_can_have_multiple_values(self):
        """Multiple labels are supported."""
        resp = JobListResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format="CSV",
            dataset_name="my-dataset",
            status=JobStatus.COMPLETED,
            idempotency_key="ik",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
            labels=["empty", "stale"],
        )
        assert resp.labels == ["empty", "stale"]

    def test_instance_id_is_optional(self):
        """instance_id defaults to None."""
        resp = JobListResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format=None,
            dataset_name="my-dataset",
            status=JobStatus.PENDING,
            idempotency_key="ik",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
        )
        assert resp.instance_id is None

    def test_instance_id_can_be_set(self):
        """instance_id can be set explicitly."""
        resp = JobListResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format=None,
            dataset_name="my-dataset",
            status=JobStatus.PENDING,
            idempotency_key="ik",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
            instance_id="inst-1",
        )
        assert resp.instance_id == "inst-1"

    def test_ckan_resource_url_defaults_to_empty_string(self):
        """ckan_resource_url is a plain field defaulting to empty string."""
        resp = JobListResponse(
            id="abc",
            resource_id="res-123",
            resource_name="test",
            resource_url=None,
            resource_format=None,
            dataset_name="my-dataset",
            status=JobStatus.PENDING,
            idempotency_key="ik",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
        )
        assert resp.ckan_resource_url == ""

    def test_ckan_resource_url_can_be_set(self):
        """ckan_resource_url can be set explicitly by the API layer."""
        resp = JobListResponse(
            id="abc",
            resource_id="res-456",
            resource_name=None,
            resource_url=None,
            resource_format=None,
            dataset_name="other-dataset",
            status=JobStatus.FAILED,
            idempotency_key="ik",
            instance_id="inst-1",
            ckan_resource_url="https://dados.pbh.gov.br/dataset/other-dataset/resource/res-456",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
        )
        assert resp.ckan_resource_url == (
            "https://dados.pbh.gov.br/dataset/other-dataset/resource/res-456"
        )


class TestJobCreate:
    def test_dataset_name_is_required(self):
        """dataset_name is a required str, not Optional."""
        with pytest.raises(Exception):  # pydantic ValidationError
            JobCreate(resource_id="r1")

    def test_resource_id_is_required(self):
        """resource_id is required."""
        with pytest.raises(Exception):
            JobCreate(dataset_name="d1")

    def test_optional_fields_default_to_none(self):
        """Optional fields default to None."""
        jc = JobCreate(resource_id="r1", dataset_name="d1")
        assert jc.resource_name is None
        assert jc.resource_url is None
        assert jc.resource_format is None
        assert jc.instance_id is None

    def test_all_fields_populated(self):
        """All fields can be set."""
        jc = JobCreate(
            resource_id="r1",
            dataset_name="d1",
            resource_name="My Resource",
            resource_url="https://example.com/data.csv",
            resource_format="CSV",
            instance_id="inst-1",
        )
        assert jc.resource_id == "r1"
        assert jc.dataset_name == "d1"
        assert jc.resource_name == "My Resource"
        assert jc.resource_url == "https://example.com/data.csv"
        assert jc.resource_format == "CSV"
        assert jc.instance_id == "inst-1"


class TestJobResponse:
    def test_job_response_inherits_fields(self):
        """JobResponse inherits all fields from JobListResponse."""
        resp = JobResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format=None,
            dataset_name="ds",
            status=JobStatus.COMPLETED,
            idempotency_key="ik",
            instance_id="inst-1",
            ckan_resource_url="https://example.com/dataset/ds/resource/r1",
            created_at=_make_datetime(),
            updated_at=_make_datetime(),
            started_at=None,
            completed_at=None,
            labels=["empty"],
            results=[],
        )
        assert resp.labels == ["empty"]
        assert resp.ckan_resource_url == ("https://example.com/dataset/ds/resource/r1")
        assert resp.instance_id == "inst-1"
        assert resp.results == []
