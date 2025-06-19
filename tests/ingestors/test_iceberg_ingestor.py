import pytest

from tests.fixtures.datasets import initial_dataset


@pytest.mark.skip(
    reason="Most of icerbeg engines does not support iceberg write yet (clickhouse, duckdb, etc)"
)
def test_full_insert(initial_dataset):
    from ckan_ingestor.iceberg_ingestor import IcebergCkanIngestor

    subject = IcebergCkanIngestor(initial_dataset)
    print(subject.ingest())
