from ckan_dagster.ckan_dagster.definitions import ckan_datasets
from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.config.s3_settings import S3Settings
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor
from tests.fixtures.datasets import initial_dataset
from tests.fixtures.minio import minio_url

def test_load_dataset(initial_dataset, minio_url):
    settings = DucklakeSettings(
        database=":memory:",
        catalog_uri=":memory:",
        data_path=S3Settings(
            endpoint=minio_url,
            url_style="path",
            access_key_id="admin",
            secret_access_key="password",
            use_ssl=False
        )
    )

    metadata_ingestor = DuckdbCkanMetadataIngestor.from_settings(settings)
    ckan_datasets(initial_dataset, metadata_ingestor)

