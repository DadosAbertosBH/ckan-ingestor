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
    BigInteger,
    Boolean,
    ForeignKey,
    Index,
    String,
    Text,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ingestor_orchestrator.db import Base
from ingestor_orchestrator.models.base import UTCDateTime, new_uuid, utcnow
from ingestor_orchestrator.models.job_status import JobStatus

if TYPE_CHECKING:
    from ingestor_orchestrator.models.ckan_data_job_result import CkanDataJobResult
    from ingestor_orchestrator.models.ckan_instance import CkanInstance


class CkanDataJob(Base):
    __tablename__ = "ckan_data_job"
    __table_args__ = (Index("ix_job_status_resource", "status", "resource_id"),)

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True, default=new_uuid)
    resource_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    resource_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    resource_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource_format: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dataset_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, values_callable=lambda obj: [e.value for e in obj]),
        default=JobStatus.PENDING,
        nullable=False,
        index=True,
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True
    )
    instance_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("ckan_instance.id"), nullable=False, index=True
    )
    ckan_url: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    datastore_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)

    instance: Mapped["CkanInstance"] = relationship(back_populates="jobs")
    results: Mapped[list["CkanDataJobResult"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        lazy="raise",
        order_by="CkanDataJobResult.created_at.desc()",
    )

    # Message broker routing metadata
    broker_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    message_stream: Mapped[str | None] = mapped_column(String(255), nullable=True)
    message_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    message_partition: Mapped[int | None] = mapped_column(nullable=True)
    message_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
