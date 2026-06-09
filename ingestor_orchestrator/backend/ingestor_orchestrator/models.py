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
import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    CHAR,
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy import (
    Enum as SAEnum,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ingestor_orchestrator.db import Base


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


def _utcnow():
    return datetime.now(timezone.utc)


def _uuid():
    return str(uuid.uuid4())


class CkanDataJob(Base):
    __tablename__ = "ckan_data_job"
    __table_args__ = (Index("ix_job_status_resource", "status", "resource_id"),)

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True, default=_uuid)
    resource_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    resource_name: Mapped[str | None] = mapped_column(String(512), nullable=True)
    resource_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource_format: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dataset_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus), default=JobStatus.PENDING, nullable=False, index=True
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow, nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    results: Mapped[list["CkanDataJobResult"]] = relationship(
        back_populates="job", cascade="all, delete-orphan", lazy="selectin"
    )


class CkanDataJobResult(Base):
    __tablename__ = "ckan_data_job_result"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True, default=_uuid)
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, nullable=False
    )

    job: Mapped["CkanDataJob"] = relationship(back_populates="results")
