# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

"""Persist the CKAN DataStore status sent to workers.

Revision ID: 014
Revises: 013
Create Date: 2026-08-27 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "ckan_data_job",
        sa.Column("datastore_active", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade():
    op.drop_column("ckan_data_job", "datastore_active")
