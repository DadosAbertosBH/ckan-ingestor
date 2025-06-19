import os

import dagster as dg
from dagster import InitResourceContext

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
from ckan_ingestor.dataset_fetcher import DatasetFetcher


class CkanFetcherResource(dg.ConfigurableResource[DatasetFetcher]):
    def create_resource(self, context: InitResourceContext) -> DatasetFetcher:
        return CkanDatasetFetcher(
            os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/")
        )
