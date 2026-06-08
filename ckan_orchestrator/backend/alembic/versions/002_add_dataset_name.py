"""Add dataset_name column to ckan_data_job

Revision ID: 002
Revises: 001
Create Date: 2026-06-08 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "ckan_data_job",
        sa.Column("dataset_name", sa.String(512), nullable=True),
    )


def downgrade():
    op.drop_column("ckan_data_job", "dataset_name")
