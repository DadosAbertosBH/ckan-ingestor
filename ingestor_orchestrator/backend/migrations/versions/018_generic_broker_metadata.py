# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

"""Use generic message broker routing metadata.

Revision ID: 018
Revises: 017
"""

from alembic import op
import sqlalchemy as sa

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ckan_data_job", sa.Column("broker_type", sa.String(32)))
    op.add_column("ckan_data_job", sa.Column("message_stream", sa.String(255)))
    op.alter_column(
        "ckan_data_job",
        "kafka_topic",
        new_column_name="message_topic",
        existing_type=sa.String(255),
        existing_nullable=True,
    )
    op.alter_column(
        "ckan_data_job",
        "kafka_partition",
        new_column_name="message_partition",
        existing_type=sa.Integer(),
        existing_nullable=True,
    )
    op.alter_column(
        "ckan_data_job",
        "kafka_offset",
        new_column_name="message_offset",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.execute(
        "UPDATE ckan_data_job SET broker_type = 'kafka' WHERE message_topic IS NOT NULL"
    )


def downgrade() -> None:
    op.alter_column(
        "ckan_data_job",
        "message_topic",
        new_column_name="kafka_topic",
        existing_type=sa.String(255),
        existing_nullable=True,
    )
    op.alter_column(
        "ckan_data_job",
        "message_partition",
        new_column_name="kafka_partition",
        existing_type=sa.Integer(),
        existing_nullable=True,
    )
    op.alter_column(
        "ckan_data_job",
        "message_offset",
        new_column_name="kafka_offset",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
    op.drop_column("ckan_data_job", "message_stream")
    op.drop_column("ckan_data_job", "broker_type")
