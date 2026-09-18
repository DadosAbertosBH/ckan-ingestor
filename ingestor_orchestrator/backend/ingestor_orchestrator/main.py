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
import asyncio
import logging
import os
import urllib.request
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from ingestor_orchestrator.api import (
    dashboard,
    datasets,
    instances,
    jobs,
    resources,
    syncs,
)
from ingestor_orchestrator.config import settings
from ingestor_orchestrator.db import async_session

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logging.getLogger("ingestor_orchestrator").setLevel(logging.INFO)
    logger.info("Starting API server")

    yield

    logger.info("API server stopped")


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
app.include_router(datasets.router)
app.include_router(dashboard.router)
app.include_router(instances.router)
app.include_router(resources.router)
app.include_router(syncs.router)

frontend_dir = "/app/frontend"
if os.path.isdir(frontend_dir):
    app.mount("/assets", StaticFiles(directory=f"{frontend_dir}/assets"), name="assets")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    checks = {"database": "ok"}

    # MySQL check
    try:
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        checks["database"] = "unreachable"

    # DuckLake check
    catalog = os.environ.get("DUCKLAKE_CATALOG_URI", "")
    if catalog:
        try:
            import duckdb

            conn = duckdb.connect(catalog)
            conn.execute("SELECT 1")
            conn.close()
            checks["ducklake"] = "ok"
        except Exception:
            checks["ducklake"] = "unreachable"

    checks["go_api"] = "ok" if await _go_ready() else "unreachable"

    if any(v != "ok" for v in checks.values()):
        return JSONResponse(status_code=503, content={"status": "error", **checks})
    return {"status": "ok", **checks}


async def _go_ready() -> bool:
    url = os.environ.get("INGEST_ORCH_GO_API_URL", "http://127.0.0.1:8081")

    def check() -> bool:
        try:
            with urllib.request.urlopen(f"{url}/ready", timeout=2) as response:
                return response.status == 200
        except Exception:
            return False

    return await asyncio.to_thread(check)


if os.path.isdir(frontend_dir):

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        file_path = os.path.join(frontend_dir, full_path)
        if full_path and os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(frontend_dir, "index.html"))


def run():
    import uvicorn

    uvicorn.run(
        "ingestor_orchestrator.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )


if __name__ == "__main__":
    run()
