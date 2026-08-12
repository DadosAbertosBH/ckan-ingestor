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
"""Drop unique constraint on ckan_data_job.idempotency_key to allow job history.

Revision ID: 006
Revises: 005
Create Date: 2026-06-09 00:00:00.000000
"""

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade():
    # Drop the unique index created implicitly by unique=True
    op.drop_index("ix_ckan_data_job_idempotency_key", "ckan_data_job")
    # Recreate as non-unique index
    op.create_index(
        "ix_ckan_data_job_idempotency_key",
        "ckan_data_job",
        ["idempotency_key"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_ckan_data_job_idempotency_key", "ckan_data_job")
    op.create_index(
        "ix_ckan_data_job_idempotency_key",
        "ckan_data_job",
        ["idempotency_key"],
        unique=True,
    )
