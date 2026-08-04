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
from unittest.mock import Mock

import duckdb
import pytest
from duckdb import DuckDBPyConnection

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


@pytest.fixture
def ingestor(
    in_memory_duckdb_conn: DuckDBPyConnection,
    ducklake_settings: DucklakeSettings,
    ckman_mock_url,
) -> DuckdbCkanDataIngestor:
    subject = DuckdbCkanDataIngestor(
        ducklake_conn=in_memory_duckdb_conn,
        document_ingestor=S3DocumentIngestor(ducklake_settings.data_path),
        datastore_reader=DatastoreReader(datastore_url=ckman_mock_url),
        csv_reader=DuckDbCsvReader(in_memory_duckdb_conn),
    )
    subject.ducklake_conn.execute("""
    CREATE TABLE IF NOT EXISTS ckan_resource_last_update
        (ckan_resource_id UUID, last_modified TIMESTAMP)
    """)
    return subject


def test_ingest_returns_false_when_all_formats_fail(
    ingestor: DuckdbCkanDataIngestor,
):
    """
    When the first format raises a DuckDB exception and all fallback
    formats also fail, the method must return False — not True.

    Regression test: the except block was calling the recursive
    ingest_ckan_data but ignoring the return value, always falling
    through to `return True`.
    """
    resource = {
        "id": "deadbeef-dead-beef-dead-beefdeadbeef",
        "name": "test-fallback-failure",
        "format": "CSV",
        "datastore_active": False,
        "url": "http://does-not-matter.because.we.mock",
    }

    # Simulate a CSV that fails all encoding/parsing attempts.
    ingestor.csv_reader.read = Mock(
        side_effect=duckdb.IOException("Failed to parse CSV from url")
    )

    result = ingestor.ingest_ckan_data(resource)
    assert result is False, (
        f"Expected False when all formats fail, got {result}. "
        "The except block is likely ignoring the recursive call's return value."
    )


def test_ingest_returns_true_on_success(
    ingestor: DuckdbCkanDataIngestor,
):
    """
    When ingestion succeeds, the method must return True.
    """
    resource = {
        "id": "cafe0000-cafe-cafe-cafe-cafe00000000",
        "name": "success-resource",
        "format": "CSV",
        "datastore_active": False,
        "url": "http://does-not-matter.because.we.mock",
    }

    import pyarrow as pa

    # Simulate a CSV that parses successfully.
    table = pa.table({"col": [1, 2, 3]})
    ingestor.csv_reader.read = Mock(return_value=table)

    result = ingestor.ingest_ckan_data(resource)
    assert result is True, f"Expected True on successful ingestion, got {result}."


def test_datastore_empty_falls_back_to_csv(
    ingestor: DuckdbCkanDataIngestor,
):
    """When datastore_active is True but the datastore returns no records,
    the ingestor must fall back to CSV instead of marking the resource empty.

    Real-world case: CKAN registers datastore_active=True but never populates
    the datastore (total=0). The actual data lives in the CSV file.
    """
    import pyarrow as pa

    resource = {
        "id": "5afe0000-5afe-5afe-5afe-5afe00000000",
        "name": "Has CSV but empty datastore",
        "format": "CSV",
        "datastore_active": True,
        "url": "http://example.com/data.csv",
    }

    # Simulate empty datastore
    ingestor.datastore_reader.read = Mock(return_value=None)
    # Simulate successful CSV parsing
    table = pa.table({"col": [1, 2, 3]})
    ingestor.csv_reader.read = Mock(return_value=table)

    result = ingestor.ingest_ckan_data(resource)
    assert result is True, (
        f"Expected fallback to CSV when datastore is empty, got {result}."
        " The resource is NOT empty — it has a CSV file with data."
    )
