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
"""Add ckan_instance table and link to ckan_data_job

Revision ID: 005
Revises: 004
Create Date: 2026-06-09 00:00:00.000000
"""

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Create ckan_instance table
    op.create_table(
        "ckan_instance",
        sa.Column("id", sa.CHAR(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
        sa.Column("url", sa.String(512), nullable=False),
        sa.Column("last_metadata_synced", sa.DateTime, nullable=True),
        sa.Column("dataset_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("resource_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )

    # 2. Add instance_id to ckan_data_job (nullable at first)
    op.add_column(
        "ckan_data_job",
        sa.Column("instance_id", sa.CHAR(36), nullable=True),
    )
    op.create_foreign_key(
        "fk_ckan_data_job_instance_id",
        "ckan_data_job",
        "ckan_instance",
        ["instance_id"],
        ["id"],
    )
    op.create_index(
        "ix_ckan_data_job_instance_id",
        "ckan_data_job",
        ["instance_id"],
    )

    # 3. Insert default instances
    pbh_id = str(uuid.uuid4())
    demo_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    ckan_instance = sa.table(
        "ckan_instance",
        sa.column("id", sa.CHAR(36)),
        sa.column("name", sa.String(255)),
        sa.column("url", sa.String(512)),
        sa.column("dataset_count", sa.Integer),
        sa.column("resource_count", sa.Integer),
        sa.column("created_at", sa.DateTime),
        sa.column("updated_at", sa.DateTime),
    )

    op.bulk_insert(
        ckan_instance,
        [
            {
                "id": pbh_id,
                "name": "PBH",
                "url": "https://dados.pbh.gov.br",
                "dataset_count": 0,
                "resource_count": 0,
                "created_at": now,
                "updated_at": now,
            },
            {
                "id": demo_id,
                "name": "Minas Gerais",
                "url": "https://dados.mg.gov.br/",
                "dataset_count": 0,
                "resource_count": 0,
                "created_at": now,
                "updated_at": now,
            },
        ],
    )

    # 4. Update existing jobs to reference the PBH instance
    op.execute(f"UPDATE ckan_data_job SET instance_id = '{pbh_id}'")

    # 5. Make instance_id NOT NULL after backfilling
    op.alter_column(
        "ckan_data_job",
        "instance_id",
        existing_type=sa.CHAR(36),
        nullable=False,
    )


def downgrade():
    op.drop_constraint(
        "fk_ckan_data_job_instance_id", "ckan_data_job", type_="foreignkey"
    )
    op.drop_index("ix_ckan_data_job_instance_id", "ckan_data_job")
    op.drop_column("ckan_data_job", "instance_id")
    op.drop_table("ckan_instance")
