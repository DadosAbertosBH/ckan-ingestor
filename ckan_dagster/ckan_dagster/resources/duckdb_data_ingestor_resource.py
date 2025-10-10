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
from contextlib import contextmanager

import dagster as dg
from dagster import InitResourceContext
from dagster_duckdb import DuckDBResource

from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


class DuckdbDataIngestorResource(dg.ConfigurableResource[DuckdbCkanDataIngestor]):
    duckdb: DuckDBResource

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        with self.duckdb.get_connection() as conn:
            # Use core nightly
            conn.install_extension("ducklake", force_install=True, repository="core_nightly")
            conn.load_extension("ducklake")
            yield DuckdbCkanDataIngestor(
                ducklake_conn=conn,
                document_ingestor=S3DocumentIngestor(
                    s3_settings=self.ducklake_settings.data_path
                ),
                datastore_reader=DatastoreReader(
                    conn=conn, dastore_url=self.ducklake_settings.datastore_url
                ),
                csv_reader=DuckDbCsvReader(conn=conn),
            )
