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
"""Tests for Pydantic schemas — labels, ckan_resource_url, and JobCreate."""

from datetime import datetime, timezone

import pytest
from ingestor_orchestrator.models import JobStatus
from ingestor_orchestrator.schemas import JobCreate, JobListResponse, JobResponse


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
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
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
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
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
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            started_at=None,
            completed_at=None,
            labels=["empty", "stale"],
        )
        assert resp.labels == ["empty", "stale"]

    def test_ckan_resource_url_is_computed(self, monkeypatch):
        """ckan_resource_url is a computed property from settings."""
        monkeypatch.setattr(
            "ingestor_orchestrator.schemas.settings.ckan_url",
            "https://dados.pbh.gov.br",
        )
        resp = JobListResponse(
            id="abc",
            resource_id="res-123",
            resource_name="test",
            resource_url=None,
            resource_format=None,
            dataset_name="my-dataset",
            status=JobStatus.PENDING,
            idempotency_key="ik",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            started_at=None,
            completed_at=None,
        )
        assert resp.ckan_resource_url == (
            "https://dados.pbh.gov.br/dataset/my-dataset/resource/res-123"
        )

    def test_ckan_resource_url_strips_trailing_slash(self, monkeypatch):
        """Trailing slash in ckan_url is stripped before building the link."""
        monkeypatch.setattr(
            "ingestor_orchestrator.schemas.settings.ckan_url",
            "https://dados.pbh.gov.br/",
        )
        resp = JobListResponse(
            id="abc",
            resource_id="res-456",
            resource_name=None,
            resource_url=None,
            resource_format=None,
            dataset_name="other-dataset",
            status=JobStatus.FAILED,
            idempotency_key="ik",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
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

    def test_all_fields_populated(self):
        """All fields can be set."""
        jc = JobCreate(
            resource_id="r1",
            dataset_name="d1",
            resource_name="My Resource",
            resource_url="https://example.com/data.csv",
            resource_format="CSV",
        )
        assert jc.resource_id == "r1"
        assert jc.dataset_name == "d1"
        assert jc.resource_name == "My Resource"
        assert jc.resource_url == "https://example.com/data.csv"
        assert jc.resource_format == "CSV"


class TestJobResponse:
    def test_job_response_inherits_labels_and_ckan_url(self, monkeypatch):
        """JobResponse inherits labels+ckan_resource_url from JobListResponse."""
        monkeypatch.setattr(
            "ingestor_orchestrator.schemas.settings.ckan_url",
            "https://dados.pbh.gov.br",
        )
        resp = JobResponse(
            id="abc",
            resource_id="r1",
            resource_name="test",
            resource_url=None,
            resource_format=None,
            dataset_name="ds",
            status=JobStatus.COMPLETED,
            idempotency_key="ik",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            started_at=None,
            completed_at=None,
            labels=["empty"],
            results=[],
        )
        assert resp.labels == ["empty"]
        assert resp.ckan_resource_url == (
            "https://dados.pbh.gov.br/dataset/ds/resource/r1"
        )
