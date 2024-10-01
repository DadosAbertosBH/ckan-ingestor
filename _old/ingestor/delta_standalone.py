from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError 
import os
import pyarrow as pa

ckan_url = os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/")
s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")
s3_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
s3_region = os.environ.get("AWS_REGION", "us-west-1")
bucket = os.environ.get("S3_BUCKET", "warehouse")

storage_options={
        "AWS_ALLOW_HTTP": "true", 
        "AWS_ENDPOINT_URL": s3_endpoint,
        "AWS_ACCESS_KEY_ID": s3_access_key_id,
        "AWS_SECRET_ACCESS_KEY": s3_secret_access_key,
        "AWS_REGION": s3_region,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "copy_if_not_exists": "header: cf-copy-destination-if-none-match: *",
    }

configuration={
    "delta.enableChangeDataFeed" : "true"
}

def merge_table(table_path: str, new_data, date_column: str):    
    try:    
        print(f"Merging table {table_path}")
        dt = DeltaTable(table_path, storage_options=storage_options)
        start_version = dt.version()
        dt.merge(source = new_data, predicate = "t.id = s.id", source_alias = "s", target_alias = "t", large_dtypes=False) \
            .when_matched_update_all(f"s.{date_column} > t.{date_column}") \
            .when_not_matched_insert_all() \
            .execute()
        return dt.load_cdf(starting_version=start_version).read_all()
    except TableNotFoundError as e:
        print(f"Creating table {table_path}")
        write_deltalake(table_path, new_data, storage_options=storage_options, configuration=configuration)
        dt = DeltaTable(table_path, storage_options=storage_options)
        # return dt.load_cdf(starting_version=0, ending_version=1)
        return None

datasets = dataset_fetcher.fetch(ckan_url)
resources = datasets["resources"].combine_chunks().flatten()
tables = pa.Table.from_struct_array(resources) \
    .drop_columns(
        [
            "cache_last_updated",
            "cache_url",
            "mimetype_inner",
            "resource_type",
        ]
    )
datasets = datasets.drop_columns("resources")

merge_table(f"s3a://{bucket}/ckan/datasets", datasets, "metadata_modified")
changes = merge_table(f"s3a://{bucket}/ckan/tables", tables, "last_modified")
print(changes[0])
