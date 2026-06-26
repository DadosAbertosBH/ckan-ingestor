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
"""Tests for /health and /ready probes."""

import pytest
from fastapi.testclient import TestClient
from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.main import app


@pytest.fixture(autouse=True)
def clear_overrides():
    yield
    app.dependency_overrides.clear()


class TestHealthEndpoint:
    def test_health_always_returns_200(self):
        """Liveness probe must not depend on external services."""
        response = TestClient(app).get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestReadyEndpoint:
    def test_ready_503_when_db_down(self):
        """Readiness probe returns 503 when database is unreachable."""

        async def broken_db():
            raise Exception("Connection refused")
            yield

        app.dependency_overrides[get_db] = broken_db
        response = TestClient(app).get("/ready")
        assert response.status_code == 503
