import logging
import os

import pyarrow

from ckan_dataset_fetcher import CkanDatasetFetcher
from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor


os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = "localhost:9000"
os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
# os.environ["DUCKLAKE_CATALOG_URI"] = "run_ducklake"
os.environ["DUCKLAKE_CATALOG_URI"] = "postgres:dbname=postgres host=localhost user=postgres password=postgres"
ckan_url = os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/")

settings = DucklakeSettings()

packages = CkanDatasetFetcher(ckan_url).fetch()
resources = packages["resources"].combine_chunks().flatten()
ckan_resources = pyarrow.Table.from_struct_array(resources)
ingestor = DuckdbCkanMetadataIngestor.from_settings()
ingestor.logger.setLevel(logging.INFO)
ingestor.ingest_dataset(packages)
ingestor.ingest_resources(ckan_resources)
ingestor.ingest_ckan_data_async(ckan_resources)
