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
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ingestor_orchestrator.db import get_db
from ingestor_orchestrator.dto import InstanceStats
from ingestor_orchestrator.repositories import (
    DashboardRepository,
    SqlAlchemyDashboardRepository,
)


def get_dashboard_repository(
    db: AsyncSession = Depends(get_db),
) -> DashboardRepository:
    return SqlAlchemyDashboardRepository(db)


router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=list[InstanceStats])
async def get_stats(
    repo: DashboardRepository = Depends(get_dashboard_repository),
):
    return await repo.get_stats()
