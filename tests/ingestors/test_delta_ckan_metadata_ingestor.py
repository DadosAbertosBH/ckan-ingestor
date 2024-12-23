from ckan_ingestor.delta_ckan_ingestor import DeltaCkanIngestor
from tests.fixtures.datasets import full_dataset

def test_full_insert(full_dataset):
    subject = DeltaCkanIngestor(full_dataset)
    print(subject.ingest())
