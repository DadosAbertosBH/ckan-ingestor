import os

from ckan_dataset_fetcher import CkanDatasetFetcher
from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from duckdb_ingestor import DuckdbCkanIngestor


os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = "localhost:9000"
os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = "localhost:9000"
os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
os.environ["DUCKLAKE_CATALOG_URI"] = "postgres:dbname=postgres host=localhost user=postgres password=postgres"
ckan_url = os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/")

settings = DucklakeSettings()

packages = CkanDatasetFetcher(ckan_url).fetch()
ingestor = DuckdbCkanIngestor(
    dataset=packages,
    datastore_url= os.path.join(ckan_url, "datastore/dump"),
    settings=settings
)
ingestor.ingest()