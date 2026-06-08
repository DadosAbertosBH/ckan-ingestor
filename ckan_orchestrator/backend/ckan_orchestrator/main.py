import asyncio
import json
import logging
from contextlib import asynccontextmanager

import nats as nats_lib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ckan_orchestrator.api import dashboard, jobs, metadata
from ckan_orchestrator.config import settings
from ckan_orchestrator.db import init_db
from ckan_orchestrator.services.scheduler import Scheduler

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    logger.info("Database tables ready")

    nc = await nats_lib.connect(settings.nats_url)
    js = nc.jetstream()
    try:
        await js.add_stream(
            name=settings.nats_stream,
            subjects=[settings.nats_subject],
        )
    except Exception:
        pass
    app.state.nats = nc
    logger.info("Connected to NATS")

    scheduler = Scheduler()
    scheduler_task = asyncio.create_task(scheduler.start())
    logger.info("Scheduler started")

    yield

    # Shutdown
    scheduler.stop()
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass
    await nc.close()
    logger.info("Disconnected from NATS")


app = FastAPI(
    title="CKAN Orchestrator",
    description="Lightweight pipeline orchestration for CKAN data ingestion",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.debug else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs.router)
app.include_router(dashboard.router)
app.include_router(metadata.router)


@app.get("/health")
async def health():
    return {"status": "ok"}


def run():
    import uvicorn

    uvicorn.run(
        "ckan_orchestrator.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )


if __name__ == "__main__":
    run()
