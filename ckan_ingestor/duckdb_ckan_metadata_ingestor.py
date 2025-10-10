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
import logging
from typing import List

import duckdb
import pyarrow

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_connection_factory import from_settings

CKAN_DATASET_TABLE = "ckan_dataset"
CKAN_RESOURCE_TABLE = "ckan_resource"


class DuckdbCkanMetadataIngestor:
    """
    This class is responsible to write the datasets and resources tables
    """

    conn: duckdb
    csv_reader: DuckDbCsvReader
    datastore_reader: DatastoreReader
    logger = logging.getLogger(__name__)

    def __init__(
            self,
            conn: duckdb.DuckDBPyConnection,
    ):
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.csv_reader = DuckDbCsvReader(conn)
        self.conn = conn

    @classmethod
    def from_settings(cls, settings: DucklakeSettings):
        return cls(
            conn=from_settings(settings=settings),
        )

    def table_exists(self, table_name):
        try:
            result = self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return len(result) > 0
        except duckdb.CatalogException:
            return False

    def ingest_dataset(self, packages: pyarrow.Table):
        ckan_datasets = packages.drop_columns("resources")
        self.conn.begin()
        self.merge_dataset(ckan_datasets, CKAN_DATASET_TABLE, "metadata_modified")
        self.conn.commit()

    def ingest_resources(self, resources: pyarrow.Table):
        self.conn.begin()
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS ckan_resource_last_update 
            (ckan_resource_id UUID, last_modified TIMESTAMP)
            """)
        self.merge_dataset(resources, CKAN_RESOURCE_TABLE, "last_modified")
        self.conn.commit()

    def merge_dataset(
            self, new_packages: pyarrow.Table, table_name: str, update_at_column: str
    ):
        if self.table_exists(table_name):
            current_packages = self.conn.table(table_name).arrow()
            new_packages = self._merge_schema(new_packages, current_packages)
            self.logger.info(f"{table_name} new dataset size: {new_packages.num_rows}")
            deleted_count = (
                self.conn.execute(f"""
                        DELETE FROM {table_name}
                        WHERE id IN (SELECT new_packages.id FROM new_packages
                            ASOF JOIN current_packages
                            ON (new_packages.id = current_packages.id AND 
                                new_packages.{update_at_column} > current_packages.{update_at_column})
                        )
                    """)
                .arrow()["Count"][0]
                .as_py()
            )
            self.logger.info(f"{table_name} rows to deleted:{deleted_count}")
            total_inserted = (
                self.conn.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * from new_packages
                        ANTI JOIN {table_name}
                        USING (id)
                    """)
                .arrow()["Count"][0]
                .as_py()
            )
            self.logger.info(f"{table_name} rows inserted:{total_inserted}")

        else:
            self.logger.info(f"{table_name} created")
            self.conn.execute(f"""
                    CREATE TABLE {table_name} AS select * from new_packages
                """)

    @staticmethod
    def _merge_schema(new_packages, current_packages):
        merged_schema = pyarrow.unify_schemas(
            [new_packages.schema, current_packages.schema], promote_options="permissive"
        )
        missing_fields = [
            f for f in merged_schema if f.name not in new_packages.column_names
        ]
        for field in missing_fields:
            new_packages = new_packages.append_column(
                field, pyarrow.nulls(new_packages.num_rows, field.type)
            )
        return new_packages.cast(merged_schema)

    def get_outdated_resources_id(self) -> List[str]:
        rows = self.conn.execute(
            """
                SELECT id, last_modified FROM ckan_resource 
                ANTI JOIN ckan_resource_last_update 
                  ON id = ckan_resource_id 
                  AND ckan_resource.last_modified::TIMESTAMP < ckan_resource_last_update.last_modified            
            """
        ).fetchall()


        return list(map(lambda row: row[0], rows))
