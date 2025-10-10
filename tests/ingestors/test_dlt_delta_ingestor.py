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
import os
from unittest.mock import patch

import pyarrow
import pytest

from ckan_ingestor.config.ducklake_settings import DucklakeSettings


@pytest.mark.skip(
    reason="skip this test for now, as we might plan to use ducklake instead of delta"
)
def test_full_insert(
    ducklake_settings: DucklakeSettings, initial_dataset: pyarrow.Table
):
    import dlt
    import dlt_ckan.ckan_source

    with patch(
        "ckan_ingestor.ckan_dataset_fetcher.CkanDatasetFetcher.fetch",
        return_value=initial_dataset,
    ):
        dlt.config["destination.filesystem.bucket_url"] = "s3://warehouse"
        dlt.secrets["destination.filesystem.credentials.endpoint_url"] = (
            f"http://{ducklake_settings.data_path.endpoint}"
        )
        dlt.secrets["destination.filesystem.credentials.aws_access_key_id"] = "admin"
        dlt.secrets["destination.filesystem.credentials.aws_secret_access_key"] = (
            "password"
        )
        os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
        pipeline = dlt.pipeline("ckan_datasets", destination="filesystem")
        pipeline.run(dlt_ckan.ckan_source.ckan(ckan_url="http://example.com"))

        assert (2, 27) == pipeline.dataset().ckan_dataset.arrow().read_all().shape
        assert (4, 23) == pipeline.dataset().ckan_resource.arrow().read_all().shape
        assert (
            3,
            14,
        ) == pipeline.dataset().f05d4bb9_3af4_4782_a85d_3b0dfe59343e.arrow().read_all().shape

        _resource_metrics = list(
            pipeline.last_trace.last_extract_info.metrics.values()
        )[0][0]["resource_metrics"]
        rows_count = pipeline.last_trace.last_normalize_info.row_counts
        assert 2 == rows_count["ckan_dataset"]
        assert 4 == rows_count["ckan_resource"]
        assert 3 == rows_count["f05d4bb9_3af4_4782_a85d_3b0dfe59343e"]

        # Run pipeline again and assert that no new rows are inserted
        pipeline = dlt.pipeline("ckan_datasets", destination="filesystem")
        _pipe_info = pipeline.run(
            dlt_ckan.ckan_source.ckan(ckan_url="http://example.com")
        )
        _resource_metrics2 = list(
            pipeline.last_trace.last_extract_info.metrics.values()
        )[0][0]["resource_metrics"]
        rows_count = pipeline.last_trace.last_normalize_info.row_counts
        # Assert that no new itens are inserted again
        assert 0 == rows_count["f05d4bb9_3af4_4782_a85d_3b0dfe59343e"]
        # Assert that no items are deleted
        assert (2, 27) == pipeline.dataset().ckan_dataset.arrow().read_all().shape
        assert (4, 23) == pipeline.dataset().ckan_resource.arrow().read_all().shape
        assert (
            3,
            14,
        ) == pipeline.dataset().f05d4bb9_3af4_4782_a85d_3b0dfe59343e.arrow().read_all().shape
