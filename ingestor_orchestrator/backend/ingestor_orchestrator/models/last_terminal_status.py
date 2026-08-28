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

from sqlalchemy import CHAR, ForeignKey, String
from sqlalchemy import Enum as SAEnum
from sqlalchemy.orm import Mapped, mapped_column

from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models.base import UTCDateTime
from ingestor_orchestrator.models.terminal_status import TerminalStatus


class LastTerminalStatus(Base):
    __tablename__ = "last_terminal_status"

    resource_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    last_terminal_job_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("ckan_data_job.id"), nullable=False
    )
    last_terminal_status: Mapped[TerminalStatus] = mapped_column(
        SAEnum(TerminalStatus, values_callable=lambda obj: [e.value for e in obj]),
        nullable=False,
    )
    last_terminal_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    last_successful_job_id: Mapped[str | None] = mapped_column(
        CHAR(36), ForeignKey("ckan_data_job.id"), nullable=True
    )
