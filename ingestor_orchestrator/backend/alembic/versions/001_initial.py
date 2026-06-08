"""Initial tables: ckan_data_job and ckan_data_job_result

Revision ID: 001
Revises:
Create Date: 2025-01-01 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None

job_status_enum = sa.Enum(
    "pending", "processing", "completed", "failed", name="jobstatus"
)


def upgrade():
    op.create_table(
        "ckan_data_job",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("resource_id", sa.String(255), nullable=False, index=True),
        sa.Column("resource_name", sa.String(512), nullable=True),
        sa.Column("resource_url", sa.Text, nullable=True),
        sa.Column("resource_format", sa.String(50), nullable=True),
        sa.Column(
            "status",
            job_status_enum,
            nullable=False,
            server_default="pending",
            index=True,
        ),
        sa.Column(
            "idempotency_key", sa.String(255), unique=True, nullable=False, index=True
        ),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.Column("started_at", sa.DateTime, nullable=True),
        sa.Column("completed_at", sa.DateTime, nullable=True),
    )
    op.create_index(
        "ix_job_status_resource", "ckan_data_job", ["status", "resource_id"]
    )

    op.create_table(
        "ckan_data_job_result",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "job_id",
            sa.String(36),
            sa.ForeignKey("ckan_data_job.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("success", sa.Boolean, nullable=False),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("error_trace", sa.Text, nullable=True),
        sa.Column("dataset_preview", sa.JSON, nullable=True),
        sa.Column("rows_processed", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )


def downgrade():
    op.drop_table("ckan_data_job_result")
    op.drop_table("ckan_data_job")
    job_status_enum.drop(op.get_bind(), checkfirst=True)
