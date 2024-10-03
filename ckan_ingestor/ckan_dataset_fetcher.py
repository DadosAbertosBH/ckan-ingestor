from ckanapi import RemoteCKAN
from pyarrow import Table
import pyarrow as pa

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class CkanDatasetFetcher(DatasetFetcher):
    url: str

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def fetch(self) -> pa.Table:
        dados_bh = RemoteCKAN(self.url)
        packages = dados_bh.action.package_search(rows=10000)["results"]

        # noinspection PyArgumentList
        df = Table.from_pylist(packages)
        datasets = df.drop_columns(
            [
                "relationships_as_subject",
                "relationships_as_object",
                "tags",
                "extras",
                "license_url"
            ]
        )
        return datasets
