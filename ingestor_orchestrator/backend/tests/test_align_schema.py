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
"""Tests for CkanDatasetFetcher."""

import pyarrow
import pytest

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher


class TestDropEmptyListColumns:
    def test_drops_all_empty_list_column(self):
        table = pyarrow.table(
            {
                "id": pyarrow.array(["a", "b"]),
                "groups": pyarrow.array([[], []]),
            }
        )
        result = CkanDatasetFetcher._drop_empty_list_columns(table)
        assert "groups" not in result.column_names

    def test_keeps_list_with_data(self):
        table = pyarrow.table(
            {
                "id": pyarrow.array(["a", "b"]),
                "groups": pyarrow.array([["g1"], []]),
            }
        )
        result = CkanDatasetFetcher._drop_empty_list_columns(table)
        assert "groups" in result.column_names

    def test_drops_all_null_list_column(self):
        groups_type = pyarrow.list_(pyarrow.string())
        table = pyarrow.table(
            {
                "id": pyarrow.array(["a", "b"]),
                "groups": pyarrow.array([None, None], type=groups_type),
            }
        )
        result = CkanDatasetFetcher._drop_empty_list_columns(table)
        assert "groups" not in result.column_names

    def test_ignores_non_list_columns(self):
        table = pyarrow.table(
            {
                "id": pyarrow.array(["a"]),
                "name": pyarrow.array([""]),
            }
        )
        result = CkanDatasetFetcher._drop_empty_list_columns(table)
        assert "name" in result.column_names


class TestAlignSchema:
    def test_missing_column_added_as_null(self):
        base_schema = pyarrow.schema(
            [
                ("id", pyarrow.string()),
                ("extra", pyarrow.string()),
            ]
        )
        table = pyarrow.table({"id": pyarrow.array(["a"])})
        result = CkanDatasetFetcher._align_schema(table, base_schema)
        assert result.column("extra")[0].as_py() is None

    def test_scalar_mismatch_falls_back_to_string(self):
        base_schema = pyarrow.schema(
            [
                ("id", pyarrow.string()),
                ("count", pyarrow.int64()),
            ]
        )
        table = pyarrow.table(
            {
                "id": pyarrow.array(["a"]),
                "count": pyarrow.array(["not_a_number"]),
            }
        )
        result = CkanDatasetFetcher._align_schema(table, base_schema)
        assert pyarrow.types.is_string(result.schema.field("count").type)

    def test_list_mismatch_with_data_raises(self):
        base_schema = pyarrow.schema(
            [
                ("id", pyarrow.string()),
                ("groups", pyarrow.list_(pyarrow.struct([("name", pyarrow.string())]))),
            ]
        )
        table = pyarrow.table(
            {
                "id": pyarrow.array(["a"]),
                "groups": pyarrow.array([["g1", "g2"]]),
            }
        )
        with pytest.raises(TypeError, match="Cannot align column"):
            CkanDatasetFetcher._align_schema(table, base_schema)

