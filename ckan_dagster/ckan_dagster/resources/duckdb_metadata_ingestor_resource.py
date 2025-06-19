import dagster as dg
from dagster import InitResourceContext

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor


class DuckdbMetadataIngestorResource(
    dg.ConfigurableResource[DuckdbCkanMetadataIngestor]
):
    ducklake_settings: dg.ResourceDependency[DucklakeSettings]

    def create_resource(
        self, context: InitResourceContext
    ) -> DuckdbCkanMetadataIngestor:
        metadata_ingestor = DuckdbCkanMetadataIngestor.from_settings(
            self.ducklake_settings
        )
        return metadata_ingestor
