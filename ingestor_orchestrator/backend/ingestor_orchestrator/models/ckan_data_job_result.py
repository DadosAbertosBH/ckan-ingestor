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

from sqlalchemy import (
    CHAR,
    JSON,
    Boolean,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models.base import UTCDateTime, new_uuid, utcnow

if TYPE_CHECKING:
    from ingestor_orchestrator.models.ckan_data_job import CkanDataJob


class CkanDataJobResult(Base):
    __tablename__ = "ckan_data_job_result"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True, default=new_uuid)
    job_id: Mapped[str] = mapped_column(
        CHAR(36),
        ForeignKey("ckan_data_job.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_trace: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_preview: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    rows_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expected_rows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    resource_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    encoding: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, nullable=False
    )

    job: Mapped["CkanDataJob"] = relationship(back_populates="results")
