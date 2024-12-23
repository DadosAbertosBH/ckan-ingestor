import pyarrow

from ckan_ingestor.iceberg_ingestor import IcebergCkanIngestor
from tests.fixtures.datasets import full_dataset


def test_full_insert(full_dataset: pyarrow.Table):
    subject = IcebergCkanIngestor(full_dataset)
    print(subject.ingest())
