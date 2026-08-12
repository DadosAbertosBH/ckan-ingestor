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
"""Shared test fixtures for orchestrator backend tests.

Uses in-memory SQLite (aiosqlite) so tests don't need a real MySQL server.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models import CkanInstance
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture(autouse=True)
def _mock_kafka():
    """Mock Kafka producer so tests don't need a real Kafka."""
    mock_producer = MagicMock()
    mock_future = MagicMock()
    record_meta = MagicMock()
    record_meta.topic = "ckan.ingest.jobs"
    record_meta.partition = 0
    record_meta.offset = 0
    mock_future.get.return_value = record_meta
    mock_producer.send.return_value = mock_future
    with patch(
        "ingestor_orchestrator.kafka_queue.get_kafka_producer",
        return_value=mock_producer,
    ):
        yield


@pytest.fixture(scope="session")
def engine():
    """Create a single async engine for the test session."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    return engine


@pytest_asyncio.fixture
async def _create_tables(engine):
    """Create all tables before each test that needs the DB."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def default_instance(db_session):
    """Create a default CkanInstance for tests."""
    instance = CkanInstance(
        id="inst-default",
        name="Default",
        url="https://dados.pbh.gov.br",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(instance)
    await db_session.flush()
    return instance


@pytest_asyncio.fixture
async def db_session(engine, _create_tables):
    """Yield an async session that rolls back after each test."""
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()
