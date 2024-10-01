import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from ckan_ingestor.config.s3_settings import S3Settings


class DeltaCkanIngestor:
    datasets: pa.Table
    bucket: str

    configuration = {
        "delta.enableChangeDataFeed": "true"
    }

    def __init__(self, dataset: pa.Table):
        s3_settings = S3Settings()
        self.datasets = dataset
        self.bucket = s3_settings.bucket
        self.storage_options = {
            "AWS_ALLOW_HTTP": "true",
            "AWS_ENDPOINT_URL": s3_settings.endpoint,
            "AWS_ACCESS_KEY_ID": s3_settings.access_key_id,
            "AWS_SECRET_ACCESS_KEY": s3_settings.secret_access_key,
            "AWS_REGION": s3_settings.region,
            "copy_if_not_exists": "header: cf-copy-destination-if-none-match: *",
        }

    def merge_table(self, table_path: str, new_data, date_column: str):
        try:
            print(f"Merging table {table_path}")
            dt = DeltaTable(table_path, storage_options=self.storage_options)
            start_version = dt.version()
            dt.merge(source=new_data, predicate="t.id = s.id", source_alias="s", target_alias="t", large_dtypes=False) \
                .when_matched_update_all(f"s.{date_column} > t.{date_column}") \
                .when_not_matched_insert_all() \
                .execute()
            return dt.load_cdf(starting_version=start_version).read_all()
        except TableNotFoundError:
            print(f"Creating table {table_path}")
            write_deltalake(table_path, new_data,
                            storage_options=self.storage_options,
                            configuration=self.configuration
                            )
            dt = DeltaTable(table_path, storage_options=self.storage_options)
            # return dt.load_cdf(starting_version=0, ending_version=1)
            return None

    def ingest(self):
        resources = self.datasets["resources"].combine_chunks().flatten()
        # noinspection PyArgumentList
        tables = pa.Table.from_struct_array(resources).drop_columns(
            [
                "cache_last_updated",
                "cache_url",
                "mimetype_inner",
                "resource_type",
            ]
        )
        datasets = self.datasets.drop_columns("resources")

        self.merge_table(f"s3a://{self.bucket}/ckan/datasets", datasets, "metadata_modified")
        return self.merge_table(f"s3a://{self.bucket}/ckan/tables", tables, "last_modified")
