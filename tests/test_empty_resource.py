# Pedalin
# Copyright (C) 2025  Pedalin
#
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
"""TDD tests for empty resource handling.

Bug: Job fails with ValueError when a datastore resource has no rows.
Fix: Return None instead of raising, mark job completed with 0 rows,
     and add an 'empty' label to the resource via MySQL resource_metadata_label.
"""

from unittest.mock import MagicMock, patch

from ckan_ingestor.datastore_reader import DatastoreReader

# ---------------------------------------------------------------------------
# Step 1: DatastoreReader.read() should return None for empty datasets
# ---------------------------------------------------------------------------


class TestDatastoreReaderEmptyResource:
    def test_read_returns_none_for_empty_dataset(self):
        """Fix: read() should return None when dataset has no rows."""
        reader = DatastoreReader(datastore_url="http://fake-datastore")

        with patch.object(DatastoreReader, "_read_json", return_value=None):
            result = reader.read("some-resource-id")

        assert result is None

    def test_read_returns_table_for_non_empty_dataset(self):
        """read() should return a Table when data exists."""
        import pyarrow

        fake_table = pyarrow.table({"col1": [1, 2]})
        reader = DatastoreReader(datastore_url="http://fake-datastore")

        # First call returns data, second returns None (no more pages)
        with patch.object(
            DatastoreReader, "_read_json", side_effect=[fake_table, None]
        ):
            result = reader.read("some-resource-id")

        assert result is not None
        assert result.num_rows == 2


# ---------------------------------------------------------------------------
# Step 2: DuckdbCkanDataIngestor handles empty resources
# ---------------------------------------------------------------------------


def _make_empty_resource(**overrides):
    resource = {
        "id": "empty-resource-id",
        "last_modified": "2021-06-11T19:00:31.375068",
        "name": "empty_resource",
        "format": "CSV",
        "datastore_active": True,
        "url": "http://fake-url/resource.csv",
    }
    resource.update(overrides)
    return resource


class TestIngestorEmptyResource:
    def test_ingest_empty_resource_does_not_raise(self):
        """When reader returns None, ingest should not raise."""
        resource = _make_empty_resource()
        mock_conn = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.return_value = None

        from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor

        ingestor = DuckdbCkanDataIngestor(
            ducklake_conn=mock_conn,
            document_ingestor=MagicMock(),
            datastore_reader=mock_reader,
            csv_reader=MagicMock(),
        )

        ingestor.ingest_ckan_data(resource)

    def test_ingest_empty_resource_returns_false_and_no_duckdb_labels(self):
        """When reader returns None, ingest should return False and NOT touch
        DuckDB label tables. Labels are handled by JobService in MySQL
        (resource_metadata_label table)."""
        resource = _make_empty_resource()
        mock_conn = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.return_value = None

        from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor

        ingestor = DuckdbCkanDataIngestor(
            ducklake_conn=mock_conn,
            document_ingestor=MagicMock(),
            datastore_reader=mock_reader,
            csv_reader=MagicMock(),
        )

        result = ingestor.ingest_ckan_data(resource)
        assert result is False

        # No DuckDB label operations — labels live in MySQL now.
        for call in mock_conn.execute.call_args_list:
            sql = call[0][0] if call[0] else ""
            assert "ckan_resource_label" not in sql, (
                "DuckDB ckan_resource_label should not be touched — "
                "labels are in MySQL resource_metadata_label"
            )

    def test_ingest_empty_resource_does_not_create_data_table(self):
        """When reader returns None, no data table should be created."""
        resource = _make_empty_resource()
        mock_conn = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.return_value = None

        from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor

        ingestor = DuckdbCkanDataIngestor(
            ducklake_conn=mock_conn,
            document_ingestor=MagicMock(),
            datastore_reader=mock_reader,
            csv_reader=MagicMock(),
        )

        ingestor.ingest_ckan_data(resource)

        # No CREATE OR REPLACE TABLE should be called for the resource
        for call in mock_conn.execute.call_args_list:
            sql = call[0][0]
            assert "CREATE OR REPLACE TABLE" not in sql
