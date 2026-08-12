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
from ingestor_orchestrator.repositories.dashboard_repository import DashboardRepository
from ingestor_orchestrator.repositories.instance_repository import InstanceRepository
from ingestor_orchestrator.repositories.job_repository import JobRepository
from ingestor_orchestrator.repositories.resource_repository import (
    ResourceDetail,
    ResourceRepository,
)
from ingestor_orchestrator.repositories.sqlalchemy_dashboard_repository import (
    SqlAlchemyDashboardRepository,
)
from ingestor_orchestrator.repositories.sqlalchemy_instance_repository import (
    SqlAlchemyInstanceRepository,
)
from ingestor_orchestrator.repositories.sqlalchemy_job_repository import (
    SqlAlchemyJobRepository,
)
from ingestor_orchestrator.repositories.sqlalchemy_resource_repository import (
    SqlAlchemyResourceRepository,
)
from ingestor_orchestrator.repositories.sqlalchemy_sync_repository import (
    SqlAlchemySyncRepository,
)
from ingestor_orchestrator.repositories.sync_repository import SyncRepository

__all__ = [
    "DashboardRepository",
    "InstanceRepository",
    "JobRepository",
    "ResourceDetail",
    "ResourceRepository",
    "SqlAlchemyDashboardRepository",
    "SqlAlchemyInstanceRepository",
    "SqlAlchemyJobRepository",
    "SqlAlchemyResourceRepository",
    "SqlAlchemySyncRepository",
    "SyncRepository",
]
