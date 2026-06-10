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
"""Add ingestion metadata columns to ckan_data_job_result.

Revision ID: 008
Revises: 007
Create Date: 2026-06-10 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "ckan_data_job_result",
        sa.Column("expected_rows", sa.Integer(), nullable=True),
    )
    op.add_column(
        "ckan_data_job_result",
        sa.Column("resource_size", sa.Integer(), nullable=True),
    )
    op.add_column(
        "ckan_data_job_result",
        sa.Column("encoding", sa.String(50), nullable=True),
    )


def downgrade():
    op.drop_column("ckan_data_job_result", "encoding")
    op.drop_column("ckan_data_job_result", "resource_size")
    op.drop_column("ckan_data_job_result", "expected_rows")
