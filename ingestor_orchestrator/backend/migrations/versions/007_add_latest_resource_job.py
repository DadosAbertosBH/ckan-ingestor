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
"""Add latest_resource_job table.

Revision ID: 007
Revises: 006
Create Date: 2026-06-10 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "latest_resource_job",
        sa.Column("resource_id", sa.String(255), primary_key=True),
        sa.Column(
            "latest_job_id",
            sa.CHAR(36),
            sa.ForeignKey("ckan_data_job.id"),
            nullable=False,
        ),
        sa.Column(
            "instance_id",
            sa.CHAR(36),
            sa.ForeignKey("ckan_instance.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("resource_name", sa.String(512), nullable=True),
        sa.Column("resource_url", sa.Text, nullable=True),
        sa.Column("resource_format", sa.String(50), nullable=True),
        sa.Column("dataset_name", sa.String(512), nullable=False),
        sa.Column(
            "status",
            sa.Enum("pending", "processing", "completed", "failed"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )


def downgrade():
    op.drop_table("latest_resource_job")
