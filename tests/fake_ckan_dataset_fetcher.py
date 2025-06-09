import pyarrow as pa

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class FakeCkanDatasetFetcher(DatasetFetcher):
    dataset: pa.Table

    def __init__(self, dataset: list[dict[str, any]]):
        self.dataset = dataset

    def do_fetch(self):
        return self.dataset
