from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from ingestor_orchestrator.config import settings

engine = create_async_engine(
    settings.database_url, echo=settings.debug, pool_pre_ping=True
)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    """FastAPI dependency that yields a database session.

    Schema migrations are handled via init container (alembic upgrade head)
    in the Helm chart, not at application startup.
    """
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()
