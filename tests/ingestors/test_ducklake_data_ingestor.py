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
import pytest
import requests
from duckdb import DuckDBPyConnection

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor
from tests.conftest import INVALID_INPUT_JSON_ID
from tests.ingestors.test_ducklake_metadata_ingestor import (
    PDF_RESOURCE_ID,
    _assert_expected_table_state,
)


@pytest.fixture
def ingestor(
    in_memory_duckdb_conn: DuckDBPyConnection,
    ducklake_settings: DucklakeSettings,
    ckman_mock_url,
) -> DuckdbCkanDataIngestor:
    subject = DuckdbCkanDataIngestor(
        ducklake_conn=in_memory_duckdb_conn,
        document_ingestor=S3DocumentIngestor(ducklake_settings.data_path),
        datastore_reader=DatastoreReader(datastore_url=ckman_mock_url),
        csv_reader=DuckDbCsvReader(in_memory_duckdb_conn),
    )
    subject.ducklake_conn.execute("""
    CREATE TABLE IF NOT EXISTS ckan_resource_last_update
        (ckan_resource_id UUID, last_modified TIMESTAMP)
        """)
    return subject


def test_s3_ingestor(ingestor: DuckdbCkanDataIngestor, dataset_with_pdf):
    subject = ingestor
    resource = dataset_with_pdf["resources"].to_pylist()[0][0]
    subject.ingest_ckan_data(resource)

    url = subject.ducklake_conn.sql(f'select url from "{PDF_RESOURCE_ID}"').fetchone()[
        0
    ]
    response = requests.get(url)
    response.raise_for_status()  # raise if error


def test_ingest_invalid_json_fallback_to_csv(ingestor: DuckdbCkanDataIngestor):
    subject = ingestor
    # noinspection PyArgumentList
    resources = {
        "id": INVALID_INPUT_JSON_ID,
        "last_modified": "2021-06-11T19:00:31.375068",
        "name": "resource_with_broken_json",
        "format": "CSV",
        "datastore_active": True,  # Try to download json first
        "url": f"http://localhost:5001/datastore/{INVALID_INPUT_JSON_ID}?format=CSV",  # fallback url to CSV
    }
    subject.ingest_ckan_data(resources)

    _assert_expected_table_state(
        subject.ducklake_conn,
        INVALID_INPUT_JSON_ID,
        1,
        inserts=1,
        deletes=0,
    )

    value = subject.ducklake_conn.execute(
        f'select x from "{INVALID_INPUT_JSON_ID}"'
    ).fetchone()[0]
    assert "from_csv" == value
