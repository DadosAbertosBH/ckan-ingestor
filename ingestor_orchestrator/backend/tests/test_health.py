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
"""Tests for /health and /ready probes."""

import pytest
from fastapi.testclient import TestClient
from ingestor_orchestrator.main import app


@pytest.fixture(autouse=True)
def clear_overrides(monkeypatch):
    async def go_ready():
        return True

    monkeypatch.setattr("ingestor_orchestrator.main._go_ready", go_ready)
    yield
    app.dependency_overrides.clear()


class TestHealthEndpoint:
    def test_health_always_returns_200(self):
        response = TestClient(app).get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestReadyEndpoint:
    def test_ready_503_when_go_api_is_disconnected(self, monkeypatch):
        """Readiness requires the Go API and its consumers."""
        monkeypatch.delenv("DUCKLAKE_CATALOG_URI", raising=False)

        async def go_down():
            return False

        monkeypatch.setattr("ingestor_orchestrator.main._go_ready", go_down)

        response = TestClient(app).get("/ready")

        assert response.status_code == 503
        assert response.json()["go_api"] == "unreachable"

    def test_ready_503_when_db_down(self, monkeypatch):
        """Readiness probe returns 503 when database is unreachable."""
        from unittest.mock import patch

        monkeypatch.delenv("DUCKLAKE_CATALOG_URI", raising=False)
        with patch(
            "ingestor_orchestrator.main.async_session",
            side_effect=Exception("Connection refused"),
        ):
            response = TestClient(app).get("/ready")
            assert response.status_code == 503
            assert response.json()["database"] == "unreachable"

    def test_ready_503_when_ducklake_down(self, monkeypatch):
        """Readiness probe returns 503 when DuckLake is unreachable."""
        from unittest.mock import patch

        monkeypatch.setenv("DUCKLAKE_CATALOG_URI", "postgres:host=bad")
        with patch("duckdb.connect", side_effect=Exception("Connection refused")):
            response = TestClient(app).get("/ready")
            assert response.status_code == 503
            assert response.json()["ducklake"] == "unreachable"
