import pyarrow

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class CkanDatasetFetcher(DatasetFetcher):
    url: str

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def fetch(self) -> pyarrow.Table:
        import duckdb

        with duckdb.connect(":memory:") as conn:
            return conn.execute(f"""
            select unnest(result, max_depth :=2) from 
            read_json('{self.url}/api/action/current_package_list_with_resources?limit=1000')
            """).arrow()
