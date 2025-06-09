import os
from unittest.mock import patch

import pyarrow
import pyarrow.compute as pc
import pytest

from ckan_ingestor.duckdb_ingestor import DuckdbCkanIngestor, CKAN_DATASET_TABLE, CKAN_RESOURCE_TABLE
from tests.fixtures.datasets import initial_dataset, dataset_with_update, dataset_with_new_row
from tests.fixtures.minio import minio_url

EXPECTED_DATASET_ROWS_SIZE = 2
EXPECTED_DATASET_COLUMNS_SIZE = 30
EXPECTED_DATASET_ROWS_WITH_INSERT_SIZE = 3
EXPECTED_RESOURCE_ROWS_SIZE = 4
EXPECTED_RESOURCE_COLUMNS_SIZE = 27

@pytest.fixture
def ducklake_ingestor(minio_url, initial_dataset):
    os.environ["DUCKLAKE_DATABASE"] = ":memory:"
    os.environ["DUCKLAKE_CATALOG_URI"] = ":memory:"
    os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = minio_url
    os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
    os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
    with patch("ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher.do_fetch", return_value=initial_dataset):
        subject = DuckdbCkanIngestor(pyarrow.Table.from_pylist(initial_dataset))
        subject.ingest()
        return subject


def test_same_dataset_does_not_generate_changes(ducklake_ingestor, initial_dataset):
    subject = ducklake_ingestor
    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        EXPECTED_DATASET_COLUMNS_SIZE,
        inserts=EXPECTED_DATASET_ROWS_SIZE,
        deletes=0
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        EXPECTED_RESOURCE_COLUMNS_SIZE,
        inserts=EXPECTED_RESOURCE_ROWS_SIZE,
        deletes=0
    )

    # Run pipeline again and assert that no new rows are inserted
    subject.ingest()

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        EXPECTED_DATASET_COLUMNS_SIZE,
        inserts=EXPECTED_DATASET_ROWS_SIZE,
        deletes=0
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        EXPECTED_RESOURCE_COLUMNS_SIZE,
        inserts=EXPECTED_RESOURCE_ROWS_SIZE,
        deletes=0
    )
    # Expected create schema  table
    snapshots = subject.conn.execute("SELECT * FROM snapshots();").arrow()
    # Expected no new snapshots (create schema and create table)
    assert snapshots.num_rows == 2


def test_update_dataset(ducklake_ingestor, dataset_with_update):
    subject = ducklake_ingestor
    # Run pipeline again and assert that one row is updated
    subject.packages = pyarrow.Table.from_pylist(dataset_with_update)
    subject.ingest()
    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        EXPECTED_DATASET_COLUMNS_SIZE,
        inserts=1,
        deletes=1
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        EXPECTED_RESOURCE_COLUMNS_SIZE,
        inserts=1,
        deletes=1
    )


def test_insert_new_row_dataset(ducklake_ingestor, dataset_with_new_row):
    subject = ducklake_ingestor
    # Run pipeline again and assert that one row is updated
    subject.packages = pyarrow.Table.from_pylist(dataset_with_new_row)
    subject.ingest()
    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE  + 1,
        EXPECTED_DATASET_COLUMNS_SIZE,
        inserts=1,
        deletes=0
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE + 1,
        EXPECTED_RESOURCE_COLUMNS_SIZE,
        inserts=1,
        deletes=0,
    )

def _assert_expected_table_state(conn, table_name, expected_rows, expected_columns, inserts=0, deletes=0):
    assert conn.table(table_name).arrow().shape == (
        expected_rows,
        expected_columns
    )

    changes = conn.execute(f"FROM table_changes('{table_name}', now(), now());").arrow()
    assert changes.filter(pc.field("change_type") == "insert").num_rows == inserts
    assert changes.filter(pc.field("change_type") == "delete").num_rows == deletes


