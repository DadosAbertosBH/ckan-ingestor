import duckdb
import pytest

from ckan_ingestor.csv_reader import DuckDbCsvReader
from tests.fixtures.datasets import latin_encoded_csv_file, non_latin1_and_non_utf8
from tests.fixtures.ckan_mock import ckman_mock_url


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
