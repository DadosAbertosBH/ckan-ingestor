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
"""Add kafka topic/partition/offset columns to ckan_data_job.

Revision ID: 010
Revises: 009
Create Date: 2026-06-11 19:30:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "ckan_data_job",
        sa.Column("kafka_topic", sa.String(255), nullable=True),
    )
    op.add_column(
        "ckan_data_job",
        sa.Column("kafka_partition", sa.Integer(), nullable=True),
    )
    op.add_column(
        "ckan_data_job",
        sa.Column("kafka_offset", sa.Integer(), nullable=True),
    )


def downgrade():
    op.drop_column("ckan_data_job", "kafka_offset")
    op.drop_column("ckan_data_job", "kafka_partition")
    op.drop_column("ckan_data_job", "kafka_topic")
