import dagster as dg
from dagster import InitResourceContext

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.config.s3_settings import S3Settings


class DucklakeSettingsResource(dg.ConfigurableResource[DucklakeSettings]):
    def create_resource(self, context: InitResourceContext) -> DucklakeSettings:
        settings = DucklakeSettings(
            database=":memory:",
            catalog_uri="postgres:dbname=postgres host=localhost user=postgres password=postgres",
            data_path=S3Settings(
                endpoint="localhost:9000",
                url_style="path",
                access_key_id="admin",
                secret_access_key="password",
                use_ssl=False,
            ),
        )
        return settings
