import os

import duckdb
import pyarrow
import pyarrow.compute as pc
import pytest
import requests

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_connection_factory import from_settings
from ckan_ingestor.duckdb_ingestor import DuckdbCkanIngestor, CKAN_DATASET_TABLE, CKAN_RESOURCE_TABLE
from ckan_ingestor.s3_pdf_ingestor import S3PdfIngestor
from tests.fixtures.ckan_mock import ckman_mock_url, INVALID_INPUT_JSON_ID
from tests.fixtures.datasets import initial_dataset, dataset_with_update, dataset_with_new_row, dataset_with_pdf
from tests.fixtures.minio import minio_url

EXPECTED_DATASET_ROWS_SIZE = 1
EXPECTED_DATASET_ROWS_WITH_INSERT_SIZE = 2
EXPECTED_RESOURCE_ROWS_SIZE = 1
PDF_RESOURCE_ID = "5a172c1c-b329-4f84-bc11-5c2fc99849e5"

@pytest.fixture
def in_memory_duckdb_conn(minio_url) -> duckdb.DuckDBPyConnection:
    os.environ["DUCKLAKE_DATABASE"] = ":memory:"
    os.environ["DUCKLAKE_CATALOG_URI"] = ":memory:"
    os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = minio_url
    os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
    os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
    return from_settings(DucklakeSettings())

@pytest.fixture
def ingestor(in_memory_duckdb_conn: duckdb.DuckDBPyConnection, ckman_mock_url):
    subject = DuckdbCkanIngestor(
        conn=in_memory_duckdb_conn,
        datastore_url=f"{ckman_mock_url}/datastore",
        pdf_ingestor=S3PdfIngestor(DucklakeSettings().data_path))
    return subject

def test_same_dataset_does_not_generate_changes(ingestor: DuckdbCkanIngestor, initial_dataset: pyarrow.Table):
    subject = ingestor
    subject.ingest(initial_dataset)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        inserts=EXPECTED_DATASET_ROWS_SIZE,
        deletes=0
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        inserts=EXPECTED_RESOURCE_ROWS_SIZE,
        deletes=0
    )

    current_snapshot = subject.conn.execute("SELECT * FROM snapshots();").arrow()

    # Run pipeline again and assert that no new rows are inserted
    subject.ingest(initial_dataset)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        inserts=EXPECTED_DATASET_ROWS_SIZE,
        deletes=0
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        inserts=EXPECTED_RESOURCE_ROWS_SIZE,
        deletes=0
    )
    # Expected create schema  table
    snapshots = subject.conn.execute("SELECT * FROM snapshots();").arrow()
    # Expected no new snapshots
    assert snapshots.num_rows == current_snapshot.num_rows


def test_update_dataset(ingestor, initial_dataset, dataset_with_update):
    subject = ingestor
    subject.ingest(initial_dataset)
    subject.ingest(dataset_with_update)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        inserts=1,
        deletes=1
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        inserts=1,
        deletes=1
    )


def test_insert_new_row_dataset(ingestor, initial_dataset, dataset_with_new_row):
    subject = ingestor
    # Run pipeline again and assert that one row is updated
    subject.ingest(initial_dataset)
    subject.ingest(dataset_with_new_row)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE + 1,
        inserts=1,
        deletes=0
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE + 1,
        inserts=1,
        deletes=0,
    )


def test_s3_ingestor(ingestor: DuckdbCkanIngestor, dataset_with_pdf):
    subject = ingestor
    subject.ingest(dataset_with_pdf)

    url = subject.conn.sql(f"select url from \"{PDF_RESOURCE_ID}\"").fetchone()[0]
    response = requests.get(url)
    response.raise_for_status()  # raise if error


def test_ingest_invalid_json_fallback_to_csv(ingestor: DuckdbCkanIngestor):
    subject = ingestor
    # noinspection PyArgumentList
    resources = pyarrow.Table.from_pylist([{
        "id": INVALID_INPUT_JSON_ID,
        "last_modified": "2021-06-11T19:00:31.375068",
        "name": "resource_with_broken_json",
        "format": "CSV",
        "datastore_active": True,  # Try to download json first
        "url": f"http://localhost:5001/datastore/{INVALID_INPUT_JSON_ID}?format=CSV",  # fallback url to CSV
    }])
    subject.ingest_ckan_data_async(resources)

    _assert_expected_table_state(
        subject.conn,
        INVALID_INPUT_JSON_ID,
        1,
        inserts=1,
        deletes=0,
    )

    value = subject.conn.execute(f'select x from "{INVALID_INPUT_JSON_ID}"').fetchone()[0]
    assert 'from_csv' == value


def _assert_expected_table_state(conn, table_name, expected_rows, inserts=0, deletes=0):
    assert conn.table(table_name).arrow().shape[0] == expected_rows
    max_snapshot = conn.execute(" SELECT MAX(snapshot_id) FROM  snapshots()").fetchone()[0]
    max_table_snapshot = conn.execute(f"""
            SELECT MAX(snapshot_id) FROM table_changes('{table_name}', 0, {max_snapshot})
        
    """).fetchone()[0]
    changes = conn.execute(f"FROM table_changes('{table_name}', {max_table_snapshot}, {max_table_snapshot});").arrow()
    assert changes.filter(pc.field("change_type") == "insert").num_rows == inserts
    assert changes.filter(pc.field("change_type") == "delete").num_rows == deletes
