import os
from unittest.mock import patch

import pyarrow
import pyarrow.compute as pc

from ckan_ingestor.duckdb_ingestor import DuckdbCkanIngestor, CKAN_DATASET_TABLE
from tests.fixtures.datasets import full_dataset, dataset_with_update
from tests.fixtures.minio import minio_url


def test_same_dataset_does_not_generate_changes(minio_url, full_dataset: pyarrow.Table):
    os.environ["DUCKLAKE_DATABASE"] = ":memory:"
    os.environ["DUCKLAKE_CATALOG_URI"] = ":memory:"
    os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = minio_url
    os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
    os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
    with patch("ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher.do_fetch", return_value=full_dataset):
        subject = DuckdbCkanIngestor(full_dataset)
        subject.ingest()

        assert subject.conn.table(CKAN_DATASET_TABLE).arrow().shape == (2, 26)
        changes = subject.conn.execute("FROM table_changes('ckan_dataset', 1, 1);").arrow()

        # Expected two insert
        assert changes.num_rows == 2
        assert changes.filter(pc.field("change_type") == "insert").num_rows == 2
        # Expected create schema  table
        snapshots = subject.conn.execute("SELECT * FROM snapshots();").arrow()
        assert snapshots.num_rows == 2
        # Run pipeline again and assert that no new rows are inserted

        subject.ingest()
        assert subject.conn.table(CKAN_DATASET_TABLE).arrow().shape == (2, 26)
        # Expected create schema  table
        snapshots = subject.conn.execute("SELECT * FROM snapshots();").arrow()
        # Expected no new snapshots
        assert snapshots.num_rows == 2

def test_update_dataset(minio_url, full_dataset: pyarrow.Table, dataset_with_update: pyarrow.Table):
    os.environ["DUCKLAKE_DATABASE"] = ":memory:"
    os.environ["DUCKLAKE_CATALOG_URI"] = ":memory:"
    os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = minio_url
    os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
    os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
    with patch("ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher.do_fetch", return_value=full_dataset):
        subject = DuckdbCkanIngestor(full_dataset)
        subject.ingest()

        assert subject.conn.table(CKAN_DATASET_TABLE).arrow().shape == (2, 26)
        changes = subject.conn.execute("FROM table_changes('ckan_dataset', 1, 1);").arrow()
        # Expected two insert
        assert changes.num_rows == 2
        assert changes.filter(pc.field("change_type") == "insert").num_rows == 2

        # Run pipeline again and assert that one row is updated
        subject.packages = dataset_with_update
        subject.ingest()
        records = subject.conn.table(CKAN_DATASET_TABLE).arrow()
        changes = subject.conn.execute("FROM table_changes('ckan_dataset', 2, 2);").arrow()
        assert records.shape == (2, 26)
        # Expected 1 insert & 1 delete
        assert changes.num_rows == 2
        assert changes.filter(pc.field("change_type") == "insert").num_rows == 1
        assert changes.filter(pc.field("change_type") == "delete").num_rows == 1
        # Assert that the metadata_modified field is updated
        assert records.to_pylist()[-1]["metadata_modified"] == dataset_with_update.to_pylist()[-1]["metadata_modified"]