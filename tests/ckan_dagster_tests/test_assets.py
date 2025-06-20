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
from ckan_dagster.ckan_dagster.definitions import ckan_datasets
from ckan_ingestor.config.s3_settings import S3Settings
from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor
from tests.fake_ckan_dataset_fetcher import FakeCkanDatasetFetcher


def test_load_dataset(initial_dataset, minio_url):
    settings = DucklakeSettings(
        database=":memory:",
        catalog_uri=":memory:",
        data_path=S3Settings(
            endpoint=minio_url,
            url_style="path",
            access_key_id="admin",
            secret_access_key="password",
            use_ssl=False,
        ),
    )
    fetcher = FakeCkanDatasetFetcher(initial_dataset)
    metadata_ingestor = DuckdbCkanMetadataIngestor.from_settings(settings)
    ckan_datasets(fetcher, metadata_ingestor)
