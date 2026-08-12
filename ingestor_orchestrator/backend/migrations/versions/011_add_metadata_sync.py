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
"""Add metadata_sync table to record sync runs.

Revision ID: 011
Revises: 010
Create Date: 2026-07-31 10:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "metadata_sync",
        sa.Column("id", sa.CHAR(36), primary_key=True),
        sa.Column("instance_id", sa.CHAR(36), nullable=False),
        sa.Column("start_time", sa.DateTime(), nullable=False),
        sa.Column("end_time", sa.DateTime(), nullable=True),
        sa.Column("total_packages", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("new_datasets", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("new_resources", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_datasets", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_resources", sa.Integer(), nullable=False, server_default="0"),
        sa.ForeignKeyConstraint(["instance_id"], ["ckan_instance.id"]),
    )
    op.create_index(
        "ix_metadata_sync_instance_id", "metadata_sync", ["instance_id"]
    )


def downgrade():
    op.drop_index("ix_metadata_sync_instance_id", table_name="metadata_sync")
    op.drop_table("metadata_sync")
