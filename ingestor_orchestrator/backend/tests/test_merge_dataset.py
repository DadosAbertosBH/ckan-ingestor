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
"""Tests for DuckdbCkanMetadataIngestor.merge_dataset — must handle schema mismatches."""

import duckdb
import pyarrow
import pytest

from ckan_ingestor.duckdb_ckan_metadata_ingestor import (
    DuckdbCkanMetadataIngestor,
)


@pytest.fixture
def conn():
    con = duckdb.connect(":memory:")
    yield con
    con.close()


class TestMergeDatasetSchemaMismatch:
    def test_insert_with_type_mismatch_is_handled(self, conn):
        """INSERT succeeds even when new columns have wider types than existing table."""
        ingestor = DuckdbCkanMetadataIngestor(conn)

        # Create existing table with notes as INT64 (e.g. from previous sync)
        _existing = pyarrow.table(
            {
                "id": pyarrow.array(["a"]),
                "name": pyarrow.array(["old_name"]),
                "notes": pyarrow.array([42], type=pyarrow.int64()),
                "metadata_modified": pyarrow.array(["2024-01-01"]),
            }
        )
        conn.execute("CREATE TABLE test_tbl AS SELECT * FROM _existing")

        # New data has notes as string (a wider type)
        new_data = pyarrow.table(
            {
                "id": pyarrow.array(["b"]),
                "name": pyarrow.array(["new_name"]),
                "notes": pyarrow.array(["Some description text"]),
                "metadata_modified": pyarrow.array(["2025-01-01"]),
            }
        )

        # This should not raise ConversionException
        ingestor.merge_dataset(new_data, "test_tbl", "metadata_modified")

        result = conn.table("test_tbl").arrow().read_all()
        assert result.num_rows >= 1

    def test_insert_with_new_columns(self, conn):
        """INSERT succeeds when new data has extra columns not in existing table."""
        ingestor = DuckdbCkanMetadataIngestor(conn)

        _existing = pyarrow.table(
            {
                "id": pyarrow.array(["a"]),
                "name": pyarrow.array(["old"]),
                "metadata_modified": pyarrow.array(["2024-01-01"]),
            }
        )
        conn.execute("CREATE TABLE test_tbl2 AS SELECT * FROM _existing")

        # New data has an extra column 'notes'
        new_data = pyarrow.table(
            {
                "id": pyarrow.array(["b"]),
                "name": pyarrow.array(["new"]),
                "notes": pyarrow.array(["extra text"]),
                "metadata_modified": pyarrow.array(["2025-01-01"]),
            }
        )

        ingestor.merge_dataset(new_data, "test_tbl2", "metadata_modified")

        result = conn.table("test_tbl2").arrow().read_all()
        assert "notes" in result.column_names


class TestMergeDatasetKeepsJsonColumns:
    """A JSON column (extras) must survive a second sync without being widened."""

    def test_second_sync_does_not_widen_json_column(self, conn):
        """Regression: sync over a table with a JSON column must not ALTER it to VARCHAR.

        DuckDB exposes JSON columns as string in Arrow, so merge_dataset sees
        extras as VARCHAR while the stored column is JSON — the widening logic
        must not ALTER the JSON column.
        """
        ingestor = DuckdbCkanMetadataIngestor(conn)

        # First sync: extras column created as JSON, as DuckDB's read_json infers
        conn.execute("""
            CREATE TABLE sync_extras_json AS
            SELECT 'a' AS id, '[]'::JSON AS extras, '2024-01-01' AS metadata_modified
        """)

        # Second sync: extras arrives as string because Arrow exposes the JSON
        # column as string (same as the production read path)
        new_data = pyarrow.table(
            {
                "id": pyarrow.array(["b"]),
                "extras": pyarrow.array(["[]"]),
                "metadata_modified": pyarrow.array(["2025-01-01"]),
            }
        )

        result = ingestor.merge_dataset(
            new_data, "sync_extras_json", "metadata_modified"
        )

        column_types = {
            r[1]: r[2]
            for r in conn.execute("PRAGMA table_info('sync_extras_json')").fetchall()
        }
        assert column_types["extras"] == "JSON"
        assert conn.table("sync_extras_json").arrow().read_all().num_rows == 2
        assert result.new == 1
        assert result.updated == 0

    def test_second_sync_with_data_rich_extras_does_not_crash(self, conn):
        """Regression: extras with {key, value} data must survive _merge_schema.

        MG-style data has extras as list<struct<key,value>>. If the existing
        table has a JSON extras column, _merge_schema must not try to cast
        list<struct> to string (ArrowNotImplementedError).
        """
        ingestor = DuckdbCkanMetadataIngestor(conn)

        conn.execute("""
            CREATE TABLE sync_extras_data AS
            SELECT 'a' AS id, '[]'::JSON AS extras, '2024-01-01' AS metadata_modified
        """)

        extras_type = pyarrow.list_(
            pyarrow.struct(
                [
                    pyarrow.field("key", pyarrow.string()),
                    pyarrow.field("value", pyarrow.string()),
                ]
            )
        )
        new_data = pyarrow.table(
            {
                "id": pyarrow.array(["b"]),
                "extras": pyarrow.array(
                    [[{"key": "tema", "value": "saude"}]],
                    type=extras_type,
                ),
                "metadata_modified": pyarrow.array(["2025-01-01"]),
            }
        )

        # Must not raise ArrowNotImplementedError
        result = ingestor.merge_dataset(
            new_data, "sync_extras_data", "metadata_modified"
        )

        column_types = {
            r[1]: r[2]
            for r in conn.execute("PRAGMA table_info('sync_extras_data')").fetchall()
        }
        assert column_types["extras"] == "JSON"
        assert result.new == 1
        assert result.updated == 0


class TestMergeDatasetCounts:
    """merge_dataset must report how many rows are new vs updated."""

    def _data(self, ids, modified_dates):
        return pyarrow.table(
            {
                "id": pyarrow.array(ids),
                "metadata_modified": pyarrow.array(modified_dates),
            }
        )

    def test_first_sync_counts_all_rows_as_new(self, conn):
        ingestor = DuckdbCkanMetadataIngestor(conn)

        result = ingestor.merge_dataset(
            self._data(["a", "b"], ["2024-01-01", "2024-01-02"]),
            "test_counts_first",
            "metadata_modified",
        )

        assert result.new == 2
        assert result.updated == 0
        assert result.updated_ids == []

    def test_same_data_returns_zero_changes(self, conn):
        ingestor = DuckdbCkanMetadataIngestor(conn)
        data = self._data(["a"], ["2024-01-01"])

        ingestor.merge_dataset(data, "test_counts_same", "metadata_modified")
        result = ingestor.merge_dataset(data, "test_counts_same", "metadata_modified")

        assert result.new == 0
        assert result.updated == 0
        assert result.updated_ids == []

    def test_updated_row_counts_as_updated(self, conn):
        ingestor = DuckdbCkanMetadataIngestor(conn)

        ingestor.merge_dataset(
            self._data(["a"], ["2024-01-01"]), "test_counts_upd", "metadata_modified"
        )
        result = ingestor.merge_dataset(
            self._data(["a"], ["2025-01-01"]),
            "test_counts_upd",
            "metadata_modified",
        )

        assert result.new == 0
        assert result.updated == 1
        assert result.updated_ids == ["a"]

    def test_new_row_counts_as_new(self, conn):
        ingestor = DuckdbCkanMetadataIngestor(conn)

        ingestor.merge_dataset(
            self._data(["a"], ["2024-01-01"]), "test_counts_new", "metadata_modified"
        )
        result = ingestor.merge_dataset(
            self._data(["a", "b"], ["2024-01-01", "2025-01-01"]),
            "test_counts_new",
            "metadata_modified",
        )

        assert result.new == 1
        assert result.updated == 0
        assert result.updated_ids == []

    def test_mixed_new_and_updated(self, conn):
        ingestor = DuckdbCkanMetadataIngestor(conn)

        ingestor.merge_dataset(
            self._data(["a"], ["2024-01-01"]), "test_counts_mix", "metadata_modified"
        )
        result = ingestor.merge_dataset(
            self._data(["a", "b"], ["2025-01-01", "2025-01-01"]),
            "test_counts_mix",
            "metadata_modified",
        )

        assert result.new == 1
        assert result.updated == 1
        assert result.updated_ids == ["a"]
