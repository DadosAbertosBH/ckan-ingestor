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
"""Make dataset_name non-nullable

Revision ID: 003
Revises: 002
Create Date: 2026-06-09 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "ckan_data_job",
        "dataset_name",
        existing_type=sa.String(512),
        nullable=False,
    )


def downgrade():
    op.alter_column(
        "ckan_data_job",
        "dataset_name",
        existing_type=sa.String(512),
        nullable=True,
    )
