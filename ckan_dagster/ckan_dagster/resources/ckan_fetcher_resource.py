import os

import dagster as dg
from dagster import InitResourceContext

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher


class CkanFetcherResource(dg.ConfigurableResource[CkanDatasetFetcher]):

    def create_resource(self, context: InitResourceContext) -> CkanDatasetFetcher:
        return CkanDatasetFetcher(os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/"))
