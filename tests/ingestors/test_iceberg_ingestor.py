import pyarrow
import pytest

from ckan_ingestor.iceberg_ingestor import IcebergCkanIngestor
from tests.fixtures.datasets import initial_dataset

@pytest.mark.skip(reason="Most of icerbeg engines does not support iceberg write yet (clickhouse, duckdb, etc)")
def test_full_insert(initial_dataset: list[dict[str, any]]):
    subject = IcebergCkanIngestor(pyarrow.Table.from_pylist(initial_dataset))
    print(subject.ingest())
