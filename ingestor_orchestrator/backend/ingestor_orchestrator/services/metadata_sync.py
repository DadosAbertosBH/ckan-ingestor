import logging

from ingestor_orchestrator.config import settings

logger = logging.getLogger(__name__)


def run_standalone():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = sync_metadata()
    print(f"Done: {result}")


if __name__ == "__main__":
    run_standalone()


def sync_metadata() -> dict:
    """
    Fetch CKAN datasets and resources, upsert into DuckLake.
    Equivalent to running ckan_datasets + ckan_resources Dagster assets.
    """
    import pyarrow

    from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
    from ckan_ingestor.config.ducklake_settings import DucklakeSettings
    from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor
    from ckan_ingestor.duckdb_connection_factory import from_settings

    ducklake_settings = DucklakeSettings()
    conn = from_settings(ducklake_settings)

    try:
        fetcher = CkanDatasetFetcher(url=settings.ckan_url)
        ingestor = DuckdbCkanMetadataIngestor(conn)

        # 1. Fetch and ingest datasets
        logger.info("Fetching CKAN datasets...")
        packages = fetcher.fetch()
        dataset_count = packages.num_rows
        logger.info(f"Found {dataset_count} packages")
        ingestor.ingest_dataset(packages)

        # 2. Extract and ingest resources
        logger.info("Ingesting resources...")
        resources_col = packages["resources"].combine_chunks().flatten()
        resources = pyarrow.Table.from_struct_array(resources_col)
        resource_count = resources.num_rows
        ingestor.ingest_resources(resources)

        logger.info(
            f"Sync complete: {dataset_count} datasets, {resource_count} resources"
        )

        return {
            "dataset_count": dataset_count,
            "resource_count": resource_count,
        }
    finally:
        conn.close()
