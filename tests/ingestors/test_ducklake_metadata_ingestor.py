import pyarrow.compute as pc

import pyarrow as pa
from duckdb import DuckDBPyConnection

from pytest import fixture

from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor, CKAN_RESOURCE_TABLE, CKAN_DATASET_TABLE

EXPECTED_DATASET_ROWS_SIZE = 1
EXPECTED_DATASET_ROWS_WITH_INSERT_SIZE = 2
EXPECTED_RESOURCE_ROWS_SIZE = 1
PDF_RESOURCE_ID = "5a172c1c-b329-4f84-bc11-5c2fc99849e5"


@fixture
def ingestor(in_memory_duckdb_conn: DuckDBPyConnection, ckman_mock_url):
    subject = DuckdbCkanMetadataIngestor(conn=in_memory_duckdb_conn)
    return subject


def test_same_dataset_does_not_generate_changes(
    ingestor: DuckdbCkanMetadataIngestor, initial_dataset: pa.Table
):
    subject = ingestor

    resources = initial_dataset["resources"].combine_chunks().flatten()
    # noinspection PyArgumentList
    ckan_resources = pa.Table.from_struct_array(resources)

    subject.ingest_dataset(initial_dataset)
    subject.ingest_resources(ckan_resources)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        inserts=EXPECTED_DATASET_ROWS_SIZE,
        deletes=0,
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        inserts=EXPECTED_RESOURCE_ROWS_SIZE,
        deletes=0,
    )

    current_snapshot = subject.conn.execute("SELECT * FROM snapshots();").arrow()

    # Run pipeline again and assert that no new rows are inserted
    subject.ingest_dataset(initial_dataset)
    subject.ingest_resources(ckan_resources)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        inserts=EXPECTED_DATASET_ROWS_SIZE,
        deletes=0,
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        inserts=EXPECTED_RESOURCE_ROWS_SIZE,
        deletes=0,
    )
    # Expected create schema  table
    snapshots = subject.conn.execute("SELECT * FROM snapshots();").arrow()
    # Expected no new snapshots
    assert snapshots.num_rows == current_snapshot.num_rows


def test_update_dataset(ingestor, initial_dataset, dataset_with_update):
    subject = ingestor
    resources = initial_dataset["resources"].combine_chunks().flatten()
    # noinspection PyArgumentList
    ckan_resources = pa.Table.from_struct_array(resources)
    subject.ingest_dataset(initial_dataset)
    subject.ingest_resources(ckan_resources)

    resources = dataset_with_update["resources"].combine_chunks().flatten()
    # noinspection PyArgumentList
    ckan_resources = pa.Table.from_struct_array(resources)
    subject.ingest_dataset(dataset_with_update)
    subject.ingest_resources(ckan_resources)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE,
        inserts=1,
        deletes=1,
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE,
        inserts=1,
        deletes=1,
    )


def test_insert_new_row_dataset(ingestor, initial_dataset, dataset_with_new_row):
    subject = ingestor
    # Run pipeline again and assert that one row is updated
    resources = initial_dataset["resources"].combine_chunks().flatten()
    # noinspection PyArgumentList
    ckan_resources = pa.Table.from_struct_array(resources)
    subject.ingest_dataset(initial_dataset)
    subject.ingest_resources(ckan_resources)

    resources = dataset_with_new_row["resources"].combine_chunks().flatten()
    # noinspection PyArgumentList
    ckan_resources = pa.Table.from_struct_array(resources)
    subject.ingest_dataset(dataset_with_new_row)
    subject.ingest_resources(ckan_resources)

    _assert_expected_table_state(
        subject.conn,
        CKAN_DATASET_TABLE,
        EXPECTED_DATASET_ROWS_SIZE + 1,
        inserts=1,
        deletes=0,
    )
    _assert_expected_table_state(
        subject.conn,
        CKAN_RESOURCE_TABLE,
        EXPECTED_RESOURCE_ROWS_SIZE + 1,
        inserts=1,
        deletes=0,
    )


def _assert_expected_table_state(conn, table_name, expected_rows, inserts=0, deletes=0):
    assert conn.table(table_name).arrow().shape[0] == expected_rows
    max_snapshot = conn.execute(
        " SELECT MAX(snapshot_id) FROM  snapshots()"
    ).fetchone()[0]
    max_table_snapshot = conn.execute(f"""
            SELECT MAX(snapshot_id) FROM table_changes('{table_name}', 0, {max_snapshot})
        
    """).fetchone()[0]
    changes = conn.execute(
        f"FROM table_changes('{table_name}', {max_table_snapshot}, {max_table_snapshot});"
    ).arrow()
    assert changes.filter(pc.field("change_type") == "insert").num_rows == inserts
    assert changes.filter(pc.field("change_type") == "delete").num_rows == deletes
