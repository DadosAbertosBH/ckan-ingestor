"""Initial migration - create ckan_data_job and ckan_data_job_result tables.

Revision ID: 001_initial
Revises:
Create Date: 2025-01-01 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str]] = None
depends_on: Union[str, Sequence[str]] = None


jobstatus = sa.Enum("pending", "processing", "completed", "failed", name="jobstatus")


def upgrade() -> None:
    op.create_table(
        "ckan_data_job",
        sa.Column("id", sa.CHAR(36), primary_key=True),
        sa.Column("resource_id", sa.String(255), nullable=False, index=True),
        sa.Column("resource_name", sa.String(512), nullable=True),
        sa.Column("resource_url", sa.Text(), nullable=True),
        sa.Column("resource_format", sa.String(50), nullable=True),
        sa.Column(
            "status", jobstatus, nullable=False, server_default="pending", index=True
        ),
        sa.Column(
            "idempotency_key",
            sa.String(255),
            nullable=False,
            unique=True,
            index=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.utcnow()
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.utcnow()
        ),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
    )

    op.create_table(
        "ckan_data_job_result",
        sa.Column("id", sa.CHAR(36), primary_key=True),
        sa.Column(
            "job_id",
            sa.CHAR(36),
            sa.ForeignKey("ckan_data_job.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("success", sa.Boolean(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_trace", sa.Text(), nullable=True),
        sa.Column("dataset_preview", sa.JSON(), nullable=True),
        sa.Column("rows_processed", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.utcnow()
        ),
    )


def downgrade() -> None:
    op.drop_table("ckan_data_job_result")
    op.drop_table("ckan_data_job")
    jobstatus.drop(op.get_bind(), checkfirst=True)
