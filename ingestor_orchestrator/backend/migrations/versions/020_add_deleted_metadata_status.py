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
# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

"""Add deleted terminal states and metadata-sync lifecycle counters.

Revision ID: 020
Revises: 019
"""

import sqlalchemy as sa
from alembic import op

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE ckan_data_job MODIFY COLUMN status "
        "ENUM('pending', 'processing', 'completed', 'failed', 'deleted') NOT NULL"
    )
    op.execute(
        "ALTER TABLE latest_resource_job MODIFY COLUMN status "
        "ENUM('pending', 'processing', 'completed', 'failed', 'outdated', 'deleted') NOT NULL"
    )
    op.execute(
        "ALTER TABLE last_terminal_status MODIFY COLUMN last_terminal_status "
        "ENUM('completed', 'failed', 'deleted') NOT NULL"
    )
    op.add_column(
        "ckan_data_job_result",
        sa.Column(
            "status",
            sa.Enum("pending", "processing", "completed", "failed", "deleted"),
            nullable=True,
        ),
    )
    op.execute(
        "UPDATE ckan_data_job_result SET status = "
        "CASE WHEN success THEN 'completed' ELSE 'failed' END"
    )
    op.alter_column("ckan_data_job_result", "status", nullable=False)
    for column in (
        "deleted_datasets",
        "deleted_resources",
    ):
        op.add_column(
            "metadata_sync",
            sa.Column(column, sa.Integer(), nullable=False, server_default="0"),
        )


def downgrade() -> None:
    for column in (
        "deleted_resources",
        "deleted_datasets",
    ):
        op.drop_column("metadata_sync", column)
    op.drop_column("ckan_data_job_result", "status")
    op.execute(
        "ALTER TABLE last_terminal_status MODIFY COLUMN last_terminal_status "
        "ENUM('completed', 'failed') NOT NULL"
    )
    op.execute(
        "ALTER TABLE latest_resource_job MODIFY COLUMN status "
        "ENUM('pending', 'processing', 'completed', 'failed', 'outdated') NOT NULL"
    )
    op.execute(
        "ALTER TABLE ckan_data_job MODIFY COLUMN status "
        "ENUM('pending', 'processing', 'completed', 'failed') NOT NULL"
    )
