import asyncio

from fastapi import APIRouter

from ckan_orchestrator.services.metadata_sync import sync_metadata

router = APIRouter(prefix="/api/metadata", tags=["metadata"])


@router.post("/sync")
async def sync():
    """Sync CKAN datasets and resources metadata into DuckLake."""
    result = await asyncio.to_thread(sync_metadata)
    return result
