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
from datetime import datetime

from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models.base import new_uuid, utcnow
from sqlalchemy import CHAR, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship


class CkanInstance(Base):
    __tablename__ = "ckan_instance"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    url: Mapped[str] = mapped_column(String(512), nullable=False)
    last_metadata_synced: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    dataset_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    resource_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )

    jobs: Mapped[list["CkanDataJob"]] = relationship(back_populates="instance")
