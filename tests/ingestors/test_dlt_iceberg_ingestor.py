import os
from unittest.mock import patch

import duckdb
from numpy.ma.testutils import assert_equal

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
        with duckdb.connect() as conn:
            conn.query(f"""
                CREATE SECRET s1 (
                    TYPE S3,
                    PROVIDER config,
                    URL_STYLE 'path',
                    KEY_ID 'admin',
                    SECRET 'password',
                    REGION 'us-central-1',
                    ENDPOINT '{minio_url[7:]}',
                    USE_SSL false
                ); 
            """)
            packges = conn.query("select * from delta_scan('s3://warehouse/ckan_packages_dataset/ckan_package')").arrow()
            assert_equal((2, 27), packges.shape)
            resource = conn.query("select * from delta_scan('s3://warehouse/ckan_packages_dataset/ckan_resource')").arrow()
            assert_equal((4, 23), resource.shape)
            licenca_engraxate = conn.query(
                "select * from delta_scan('s3://warehouse/ckan_packages_dataset/f05d4bb9_3af4_4782_a85d_3b0dfe59343e')").arrow()
            assert_equal((3, 14), licenca_engraxate.shape)

        rows_count = pipeline.last_trace.last_normalize_info.row_counts
        assert_equal(2, rows_count['ckan_package'])
        assert_equal(4, rows_count['ckan_resource'])
        assert_equal(3, rows_count['f05d4bb9_3af4_4782_a85d_3b0dfe59343e'])

        # Run pipeline again and assert that no new row are inserted
        pipeline = dlt.pipeline(
            "ckan_packages",
            destination="filesystem"
        )
        pipeline.run(dlt_ckan.ckan_source.ckan(ckan_url="http://example.com"))
        rows_count = pipeline.last_trace.last_normalize_info.row_counts
        assert_equal(0, rows_count['ckan_package'])
        assert_equal(0, rows_count['ckan_resource'])
        assert_equal(0, rows_count['f05d4bb9_3af4_4782_a85d_3b0dfe59343e'])
