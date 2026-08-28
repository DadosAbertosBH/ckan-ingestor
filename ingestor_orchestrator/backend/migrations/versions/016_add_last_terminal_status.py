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
"""Add last terminal status projection per resource."""

import sqlalchemy as sa
from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        ALTER TABLE latest_resource_job
        MODIFY COLUMN status ENUM('pending', 'processing', 'completed', 'failed', 'outdated')
        NOT NULL
        """
    )
    op.create_table(
        "last_terminal_status",
        sa.Column("resource_id", sa.String(255), primary_key=True),
        sa.Column(
            "last_terminal_job_id",
            sa.String(36),
            sa.ForeignKey("ckan_data_job.id"),
            nullable=False,
        ),
        sa.Column(
            "last_terminal_status",
            sa.Enum("completed", "failed", name="lastterminalstatus"),
            nullable=False,
        ),
        sa.Column("last_terminal_at", sa.DateTime, nullable=False),
        sa.Column(
            "last_successful_job_id",
            sa.String(36),
            sa.ForeignKey("ckan_data_job.id"),
            nullable=True,
        ),
    )

    # Backfill the latest terminal outcome for resources with existing history.
    op.execute(
        """
        INSERT INTO last_terminal_status (
            resource_id, last_terminal_job_id, last_terminal_status,
            last_terminal_at, last_successful_job_id
        )
        SELECT
            j.resource_id,
            j.id,
            j.status,
            COALESCE(j.completed_at, j.updated_at, j.created_at),
            (
                SELECT success_job.id
                FROM ckan_data_job success_job
                WHERE success_job.resource_id = j.resource_id
                  AND success_job.status = 'completed'
                ORDER BY success_job.completed_at DESC, success_job.created_at DESC
                LIMIT 1
            )
        FROM ckan_data_job j
        WHERE j.status IN ('completed', 'failed')
          AND NOT EXISTS (
              SELECT 1
              FROM ckan_data_job newer
              WHERE newer.resource_id = j.resource_id
                AND newer.status IN ('completed', 'failed')
                AND (
                    newer.completed_at > j.completed_at
                    OR (
                        newer.completed_at = j.completed_at
                        AND newer.created_at > j.created_at
                    )
                )
          )
        """
    )


def downgrade():
    op.drop_table("last_terminal_status")
    op.execute(
        """
        ALTER TABLE latest_resource_job
        MODIFY COLUMN status ENUM('pending', 'processing', 'completed', 'failed')
        NOT NULL
        """
    )
