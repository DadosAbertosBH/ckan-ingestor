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
