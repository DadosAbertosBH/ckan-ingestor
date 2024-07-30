from deltalake import DeltaTable, write_deltalake
import dataset_fetcher
import os

s3_endpoint = os.environ.get("AWS_ENDPOINT", "http://localhost:9000")
s3_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
s3_region = os.environ.get("AWS_REGION", "us-west-1")
ckan_url = os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/")
bucket = os.environ.get("S3_BUCKET", "public-datasets")

storage_options={
        "AWS_ALLOW_HTTP": "true", 
        "AWS_ENDPOINT": s3_endpoint,
        "AWS_ACCESS_KEY_ID": s3_access_key_id,
        "AWS_SECRET_ACCESS_KEY": s3_secret_access_key,
        "AWS_REGION": s3_region,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


df = dataset_fetcher.fetch(ckan_url)

table_path = f"s3a://{bucket}/ckan"

try:    
    dt = DeltaTable(table_path, storage_options=storage_options)
    print("Merging table")
    dt.merge(source = df, predicate = "t.id = s.id", source_alias = "s", target_alias = "t", large_dtypes=False) \
        .when_matched_update_all("s.metadata_modified > t.metadata_modified") \
        .when_not_matched_insert_all() \
        .execute()
except:
    print("Creating table")
    write_deltalake(table_path, df, storage_options=storage_options)




