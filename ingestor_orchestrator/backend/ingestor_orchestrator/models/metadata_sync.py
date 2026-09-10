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

from sqlalchemy import CHAR, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models.base import UTCDateTime, new_uuid, utcnow

if TYPE_CHECKING:
    from ingestor_orchestrator.models.ckan_instance import CkanInstance


class MetadataSync(Base):
    """Record of a metadata sync run for a CKAN instance."""

    __tablename__ = "metadata_sync"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True, default=new_uuid)
    instance_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("ckan_instance.id"), nullable=False, index=True
    )
    start_time: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, nullable=False
    )
    end_time: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    total_packages: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    new_datasets: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    new_resources: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_datasets: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_resources: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    deleted_datasets: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    deleted_resources: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    instance: Mapped["CkanInstance"] = relationship(back_populates="syncs")
