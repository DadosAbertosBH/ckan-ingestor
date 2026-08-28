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
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import CHAR, ForeignKey, String, Text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models.base import UTCDateTime, utcnow
from ingestor_orchestrator.models.resource_status import ResourceStatus

if TYPE_CHECKING:
    from ingestor_orchestrator.models.ckan_data_job import CkanDataJob
    from ingestor_orchestrator.models.ckan_instance import CkanInstance


class LatestResourceJob(Base):
    __tablename__ = "latest_resource_job"

    resource_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    latest_job_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("ckan_data_job.id"), nullable=False
    )
    instance_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("ckan_instance.id"), nullable=False, index=True
    )
    resource_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    resource_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource_format: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dataset_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[ResourceStatus] = mapped_column(
        SAEnum(ResourceStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False
    )

    job: Mapped["CkanDataJob"] = relationship()
    instance: Mapped["CkanInstance"] = relationship()
