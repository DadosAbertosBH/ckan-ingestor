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
"""Shared test fixtures for orchestrator backend tests.

Uses in-memory SQLite (aiosqlite) so tests don't need a real MySQL server.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models import CkanInstance
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture(autouse=True)
def _mock_iggy():
    """Mock Iggy publisher so unit tests don't need a real broker."""
    from ingestor_orchestrator.iggy_queue import PublishMetadata

    mock_bus = AsyncMock()
    mock_bus.publish.return_value = PublishMetadata(
        broker_type="iggy",
        stream="ckan-ingestor",
        topic="jobs",
        partition=0,
        offset=None,
    )

    async def publish(topic, _payload, *, key):
        return PublishMetadata(
            broker_type="iggy",
            stream="ckan-ingestor",
            topic=topic,
            partition=0,
            offset=None,
        )

    mock_bus.publish.side_effect = publish
    mock_consumer = AsyncMock()
    mock_bus.result_consumer.return_value = mock_consumer
    mock_bus.metadata_sync_result_consumer.return_value = mock_consumer
    with (
        patch(
            "ingestor_orchestrator.iggy_queue.get_iggy_bus",
            return_value=mock_bus,
        ),
        patch(
            "ingestor_orchestrator.result_consumer.get_iggy_bus",
            return_value=mock_bus,
        ),
        patch(
            "ingestor_orchestrator.main.get_iggy_bus",
            return_value=mock_bus,
        ),
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
