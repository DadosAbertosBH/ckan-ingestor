import pyarrow
import pyarrow as pa

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class FakeCkanDatasetFetcher(DatasetFetcher):
    dataset: pa.Table

    def __init__(self, dataset: pyarrow.Table):
        self.dataset = dataset

    def fetch(self):
        return self.dataset
