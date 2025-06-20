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
import uuid
from contextlib import contextmanager

import dagster as dg
import duckdb
import sherlock
from dagster import InitResourceContext
from sherlock import Lock

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


class DuckdbDataIngestorResource(dg.ConfigurableResource[DuckdbCkanDataIngestor]):
    metadata_ingestor: dg.ResourceDependency[DuckdbCkanMetadataIngestor]
    ducklake_settings: dg.ResourceDependency[DucklakeSettings]

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        connection_config = {
            "memory_limit": "1GB",
            "threads": 1,
        }
        sherlock.configure(
            expire=600, timeout=600, retry_interval=0.1, backend=sherlock.backends.REDIS
        )
        with duckdb.connect(
            f":memory:{uuid.uuid4()}", config=connection_config
        ) as local_conn:
            yield DuckdbCkanDataIngestor(
                lock=Lock("my_lock"),
                ducklake_conn=self.metadata_ingestor.conn,
                document_ingestor=S3DocumentIngestor(
                    s3_settings=self.ducklake_settings.data_path
                ),
                datastore_reader=DatastoreReader(
                    conn=local_conn, dastore_url=self.ducklake_settings.datastore_url
                ),
                csv_reader=DuckDbCsvReader(conn=local_conn),
            )
