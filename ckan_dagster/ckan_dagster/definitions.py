import dagster as dg
import pyarrow

from ckan_dagster.ckan_dagster.resources.ckan_fetcher_resource import (
    CkanFetcherResource,
)
from ckan_dagster.ckan_dagster.resources.duckdb_data_ingestor_resource import (
    DuckdbDataIngestorResource,
)
from ckan_dagster.ckan_dagster.resources.duckdb_metadata_ingestor_resource import (
    DuckdbMetadataIngestorResource,
)
from ckan_dagster.ckan_dagster.resources.ducklake_settings_resource import (
    DucklakeSettingsResource,
)
from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
from ckan_ingestor.dataset_fetcher import DatasetFetcher
from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor


@dg.asset()
def ckan_datasets(
    dataset_fetcher: dg.ResourceParam[DatasetFetcher],
    metadata_ingestor: dg.ResourceParam[DuckdbCkanMetadataIngestor],
):
    packages = dataset_fetcher.fetch()
    metadata_ingestor.ingest_dataset(packages)
    preview_df = (
        metadata_ingestor.conn.execute(
            "select id, name, url, metadata_modified from ckan_dataset limit 10"
        )
        .df()
        .to_markdown(index=False)
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(packages.num_rows),
            "preview": preview_df,
        }
    )


@dg.asset(deps=[ckan_datasets])
def ckan_resources(
    dataset_fetcher: dg.ResourceParam[CkanDatasetFetcher],
    metadata_ingestor: dg.ResourceParam[DuckdbCkanMetadataIngestor],
):
    resources = dataset_fetcher.fetch()["resources"].combine_chunks().flatten()
    # noinspection PyArgumentList
    ckan_resources = pyarrow.Table.from_struct_array(resources)
    metadata_ingestor.ingest_resources(ckan_resources)
    count = (
        metadata_ingestor.conn.execute("select count(*) from ckan_resource").fetchone()
    )[0]
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(
                metadata_ingestor.conn.execute(
                    "select id, name, url, last_modified from ckan_resource limit 10"
                )
                .fetchdf()
                .to_markdown(index=False)
            ),
        }
    )


resource_partitions = dg.DynamicPartitionsDefinition(name="resources")


@dg.asset(deps=[ckan_resources], partitions_def=resource_partitions)
def ckan_data(
    context: dg.AssetExecutionContext,
    ingestor: dg.ResourceParam[DuckdbCkanDataIngestor],
):
    resource_id = context.partition_key
    resource = (
        ingestor.ducklake_conn.execute(
            "select * from ckan_resource where id = ?", (resource_id,)
        )
        .arrow()
        .to_pylist()[0]
    )

    ingestor.ingest_ckan_data(resource)
    count = ingestor.ducklake_conn.execute(
        f'select count(*) from "{resource_id}"."{resource_id}"'
    ).fetchone()[0]
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(
                ingestor.ducklake_conn.execute(
                    f'select * from "{resource_id}"."{resource_id}" limit 10'
                )
                .fetchdf()
                .to_markdown(index=False)
            ),
        }
    )


ckan_data_request_job = dg.define_asset_job(
    name="ckan_data_job",
    selection=dg.AssetSelection.assets("ckan_data"),
)


@dg.asset_sensor(
    asset_key=dg.AssetKey("ckan_resources"),
    job=ckan_data_request_job,
    minimum_interval_seconds=60 * 60 * 12,  # 12 hours
)
def ckan_data_request_sensor(
    context: dg.SensorEvaluationContext,
    metadata_ingestor: dg.ResourceParam[DuckdbCkanMetadataIngestor],
):
    skipped = []
    requested = []
    resources = metadata_ingestor.conn.execute(
        "select id, last_modified from ckan_resource"
    ).fetchall()
    resource_ids = [r[0] for r in resources]
    for r in resources:
        r_id, r_last_modified = r
        if metadata_ingestor.table_is_up_to_date(r_id, r_last_modified):
            skipped.append(
                dg.SkipReason(
                    f"Asset {r_id} is up to date, last_mofied = {r_last_modified}"
                )
            )
        else:
            requested.append(
                dg.RunRequest(
                    run_key=f"adhoc_ckan_data_{r_id}_{r_last_modified}",
                    partition_key=r_id,
                )
            )
    context.log.info(
        f"Finished processing {len(resources)} resources, requested={len(requested)}, skipped={len(skipped)}"
    )
    return dg.SensorResult(
        run_requests=requested,
        skip_reasons=skipped,
        dynamic_partitions_requests=[
            resource_partitions.build_add_request(resource_ids)
        ],
    )


ducklake_settings = DucklakeSettingsResource()
metadata_ingestor = DuckdbMetadataIngestorResource(ducklake_settings=ducklake_settings)
defs = dg.Definitions(
    assets=[ckan_datasets, ckan_resources, ckan_data],
    jobs=[ckan_data_request_job],
    sensors=[ckan_data_request_sensor],
    resources={
        "dataset_fetcher": CkanFetcherResource(),
        "metadata_ingestor": metadata_ingestor,
        "ingestor": DuckdbDataIngestorResource(
            metadata_ingestor=metadata_ingestor, ducklake_settings=ducklake_settings
        ),
    },
)
