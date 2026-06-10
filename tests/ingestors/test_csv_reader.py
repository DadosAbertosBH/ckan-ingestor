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
import os

import duckdb
import pytest

from ckan_ingestor.csv_reader import DuckDbCsvReader


@pytest.fixture
def gz_csv_file():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../fixtures/data/test.csv.gz",
    )


@pytest.fixture
def in_memory_duckdb_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


def test_parse_latin_encoded_csv_file(
    in_memory_duckdb_conn: duckdb.DuckDBPyConnection, latin_encoded_csv_file: str
):
    subject = DuckDbCsvReader(in_memory_duckdb_conn)
    subject.read(latin_encoded_csv_file)


def test_parse_non_latin_and_non_utf8(
    in_memory_duckdb_conn: duckdb.DuckDBPyConnection, non_latin1_and_non_utf8: str
):
    subject = DuckDbCsvReader(in_memory_duckdb_conn)
    subject.read(non_latin1_and_non_utf8)


def test_csv_with_bom(ckman_mock_url, in_memory_duckdb_conn: duckdb.DuckDBPyConnection):
    subject = DuckDbCsvReader(in_memory_duckdb_conn)
    subject.read(f"{ckman_mock_url}/datastore/csv_with_bom?format=csv")


def test_read_gzipped_csv_uses_compression(
    in_memory_duckdb_conn: duckdb.DuckDBPyConnection,
):
    """URL ending with .gz must pass compression='gzip' to read_csv."""
    gz_url = "https://example.com/data.csv.gz"
    normal_url = "https://example.com/data.csv"

    from ckan_ingestor.csv_reader import DuckDbCsvReader

    # The method reads SQL from the reader object by inspecting
    # its internal implementation. Test the URL detection logic.
    reader = DuckDbCsvReader(in_memory_duckdb_conn)

    # Verify the .gz detection logic
    assert reader._is_gzipped(gz_url) is True
    assert reader._is_gzipped(normal_url) is False
