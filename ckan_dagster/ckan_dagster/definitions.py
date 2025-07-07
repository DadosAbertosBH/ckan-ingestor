# Pedalin
# Copyright (C) 2025  Pedalin

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import dagster as dg
import dagster_celery
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
        f'select count(*) from "{resource_id}"'
    ).fetchone()[0]
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(
                ingestor.ducklake_conn.execute(
                    f'select * from "{resource_id}" limit 10'
                )
                .fetchdf()
                .to_markdown(index=False)
            ),
        }
    )


ckan_data_job = dg.define_asset_job(
    executor_def=dg.in_process_executor,
    name="ckan_data_job",
    selection=dg.AssetSelection.assets("ckan_data"),
)


@dg.sensor(
    job=ckan_data_job,
    minimum_interval_seconds=60 * 60 * 8,  # 8 hours
)
def ckan_data_request_sensor(
        _context: dg.SensorEvaluationContext,
        metadata_ingestor: dg.ResourceParam[DuckdbCkanMetadataIngestor],
) -> dg.SensorResult:
    resources_id = metadata_ingestor.get_outdated_resources_id()
    runs = [dg.RunRequest(run_key=r_id, partition_key=r_id) for r_id in resources_id]
    return dg.SensorResult(
        run_requests=runs,
        dynamic_partitions_requests=[
            resource_partitions.build_add_request(resources_id)
        ],
    )


ducklake_settings = DucklakeSettingsResource()
metadata_ingestor = DuckdbMetadataIngestorResource(ducklake_settings=ducklake_settings)
defs = dg.Definitions(
    executor=dagster_celery.celery_executor.configured(
        {
            "broker": "pyamqp://test:test@dagster-rabbitmq:5672/",
            "backend": "rpc://"
        }
    ),
    assets=[ckan_datasets, ckan_resources, ckan_data],
    jobs=[ckan_data_job],
    sensors=[ckan_data_request_sensor],
    resources={
        "dataset_fetcher": CkanFetcherResource(),
        "metadata_ingestor": metadata_ingestor,
        "ingestor": DuckdbDataIngestorResource(
            metadata_ingestor=metadata_ingestor, ducklake_settings=ducklake_settings
        ),
    },
)
