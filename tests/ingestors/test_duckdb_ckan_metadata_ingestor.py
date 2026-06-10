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
import duckdb
import pytest

from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor


def _handler_count():
    """How many handlers are attached to the ingestor's logger."""
    logger = DuckdbCkanMetadataIngestor.logger
    return len(logger.handlers)


@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(database=":memory:")
    yield conn
    conn.close()


@pytest.fixture
def ingestor(duckdb_conn):
    return DuckdbCkanMetadataIngestor(duckdb_conn)


def test_get_outdated_resources_id(ingestor):
    conn = ingestor.conn
    # Cria as tabelas necessárias
    conn.execute("""
        CREATE TABLE ckan_resource (
            id varchar(50), last_modified varchar(50)
        )
    """)
    conn.execute("""
        CREATE TABLE ckan_resource_last_update (
            ckan_resource_id varchar(50), last_modified TIMESTAMP
        )
    """)
    # Insere dados de teste
    conn.execute("""
        INSERT INTO ckan_resource VALUES
        ('11111111-1111-1111-1111-111111111111', '2024-01-01 10:00:00'),
        ('22222222-2222-2222-2222-222222222222', '2024-01-02 10:00:00'),
        ('33333333-3333-3333-3333-333333333333', '2024-01-03 10:00:00')
    """)
    conn.execute("""
        INSERT INTO ckan_resource_last_update VALUES
        ('11111111-1111-1111-1111-111111111111', '2023-12-31 09:00:00'),
        ('22222222-2222-2222-2222-222222222222', '2025-01-02 09:00:00')
    """)
    # O recurso 111... está desatualizado (last_modified > last_update),
    # 222... tá atualizado (last_modified < last_update)
    # 333... nunca foi processado
    result = ingestor.get_outdated_resources_id()
    assert "11111111-1111-1111-1111-111111111111" in result
    assert "33333333-3333-3333-3333-333333333333" in result
    assert "22222222-2222-2222-2222-222222222222" not in result


def test_multiple_instances_do_not_accumulate_handlers(duckdb_conn):
    """Creating multiple instances must not duplicate log handlers."""
    before = _handler_count()

    # Create 3 instances — should not add 3 handlers
    DuckdbCkanMetadataIngestor(duckdb_conn)
    DuckdbCkanMetadataIngestor(duckdb_conn)
    DuckdbCkanMetadataIngestor(duckdb_conn)

    after = _handler_count()

    # At most 1 handler added total (the first instance)
    assert after <= before + 1, (
        f"Handler count grew from {before} to {after} — should be at most {before + 1}"
    )
