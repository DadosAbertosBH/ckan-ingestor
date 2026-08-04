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
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import CkanInstanceResponse, InstanceCreate
from ingestor_orchestrator.models import CkanInstance
from ingestor_orchestrator.repositories.instance_repository import InstanceRepository
from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
    SqlAlchemyInstanceRepository,
)

router = APIRouter(prefix="/api/instances", tags=["instances"])


def get_instance_repository(
    db: AsyncSession = Depends(get_db),
) -> InstanceRepository:
    return SqlAlchemyInstanceRepository(db)


@router.get("/", response_model=list[CkanInstanceResponse])
async def list_instances(
    repo: InstanceRepository = Depends(get_instance_repository),
):
    return await repo.list_instances()


@router.get("/{instance_id}", response_model=CkanInstanceResponse)
async def get_instance(
    instance_id: str,
    repo: InstanceRepository = Depends(get_instance_repository),
):
    instance = await repo.get_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    return instance


@router.post("/", response_model=CkanInstanceResponse, status_code=201)
async def create_instance(
    data: InstanceCreate,
    repo: InstanceRepository = Depends(get_instance_repository),
    db: AsyncSession = Depends(get_db),
):
    instance = CkanInstance(name=data.name, url=data.url)
    instance = await repo.create_instance(instance)
    await db.commit()
    await db.refresh(instance)
    return instance


@router.delete("/{instance_id}", status_code=204)
async def delete_instance(
    instance_id: str,
    repo: InstanceRepository = Depends(get_instance_repository),
    db: AsyncSession = Depends(get_db),
):
    deleted = await repo.delete_instance(instance_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Instance not found")
    await db.commit()
