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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import CkanInstanceResponse, InstanceCreate
from ingestor_orchestrator.models import CkanInstance

router = APIRouter(prefix="/api/instances", tags=["instances"])


@router.get("/", response_model=list[CkanInstanceResponse])
async def list_instances(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(CkanInstance).order_by(CkanInstance.name))
    return result.scalars().all()


@router.get("/{instance_id}", response_model=CkanInstanceResponse)
async def get_instance(instance_id: str, db: AsyncSession = Depends(get_db)):
    instance = await db.get(CkanInstance, instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    return instance


@router.post("/", response_model=CkanInstanceResponse, status_code=201)
async def create_instance(data: InstanceCreate, db: AsyncSession = Depends(get_db)):
    instance = CkanInstance(name=data.name, url=data.url)
    db.add(instance)
    await db.commit()
    await db.refresh(instance)
    return instance


@router.delete("/{instance_id}", status_code=204)
async def delete_instance(instance_id: str, db: AsyncSession = Depends(get_db)):
    instance = await db.get(CkanInstance, instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    await db.delete(instance)
    await db.commit()
