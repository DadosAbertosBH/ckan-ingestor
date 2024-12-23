import os
from unittest.mock import patch
from tests.fixtures.datasets import full_dataset

import dlt
import pyarrow
from tests.fixtures.minio import minio_url

import dlt_ckan.ckan_source

def test_full_insert(minio_url, full_dataset: pyarrow.Table):
    with patch("ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher.do_fetch", return_value=full_dataset):
        dlt.config["destination.filesystem.bucket_url"] = "s3://warehouse"
        dlt.secrets["destination.filesystem.credentials.endpoint_url"] = minio_url
        os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
        pipeline = dlt.pipeline(
            "ckan_packages",
            destination="filesystem"
        )
        pipeline.run(dlt_ckan.ckan_source.ckan(ckan_url = "http://example.com"))
        print("Hi")