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
"""TDD tests for empty resource handling.

Bug: Job fails with ValueError when a datastore resource has no rows.
Fix: Return None instead of raising, mark job completed with 0 rows,
     and add an 'empty' label to the resource via MySQL resource_metadata_label.
"""

from unittest.mock import patch

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
