import os
from unittest.mock import patch

import dlt

import pyarrow
import pytest

import dlt_ckan.ckan_source
from tests.fixtures.datasets import initial_dataset
from tests.fixtures.minio import minio_url

@pytest.mark.skip(reason="skip this test for now, as we might plan to use ducklake instead of delta")
def test_full_insert(minio_url, initial_dataset: list[dict[str, any]]):
    with patch("ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher.do_fetch", return_value=initial_dataset):
        dlt.config["destination.filesystem.bucket_url"] = "s3://warehouse"
        dlt.secrets["destination.filesystem.credentials.endpoint_url"] = f"http://{minio_url}"
        dlt.secrets["destination.filesystem.credentials.aws_access_key_id"] = "admin"
        dlt.secrets["destination.filesystem.credentials.aws_secret_access_key"] = "password"
        os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
        pipeline = dlt.pipeline(
            "ckan_datasets",
            destination="filesystem"
        )
        pipeline.run(dlt_ckan.ckan_source.ckan(ckan_url = "http://example.com"))

        assert (2, 27) == pipeline.dataset().ckan_dataset.arrow().shape
        assert (4, 23) == pipeline.dataset().ckan_resource.arrow().shape
        assert (3, 14) == pipeline.dataset().f05d4bb9_3af4_4782_a85d_3b0dfe59343e.arrow().shape

        _resource_metrics = list(pipeline.last_trace.last_extract_info.metrics.values())[0][0]["resource_metrics"]
        rows_count = pipeline.last_trace.last_normalize_info.row_counts
        assert 2 == rows_count['ckan_dataset']
        assert 4 == rows_count['ckan_resource']
        assert 3 == rows_count['f05d4bb9_3af4_4782_a85d_3b0dfe59343e']

        # Run pipeline again and assert that no new rows are inserted
        pipeline = dlt.pipeline(
            "ckan_datasets",
            destination="filesystem"
        )
        _pipe_info = pipeline.run(dlt_ckan.ckan_source.ckan(ckan_url="http://example.com"))
        _resource_metrics2 = list(pipeline.last_trace.last_extract_info.metrics.values())[0][0]["resource_metrics"]
        rows_count = pipeline.last_trace.last_normalize_info.row_counts
        # Assert that no new itens are inserted again
        assert 0 == rows_count['f05d4bb9_3af4_4782_a85d_3b0dfe59343e']
        # Assert that no items are deleted
        assert (2, 27) == pipeline.dataset().ckan_dataset.arrow().shape
        assert (4, 23) == pipeline.dataset().ckan_resource.arrow().shape
        assert (3, 14) == pipeline.dataset().f05d4bb9_3af4_4782_a85d_3b0dfe59343e.arrow().shape
