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
        if not self.logger.hasHandlers():
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
            current_packages = self.conn.table(table_name).arrow().read_all()
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
                .arrow()
                .read_all()["Count"][0]
                .as_py()
            )
            self.logger.info(f"{table_name} rows to deleted:{deleted_count}")

            # Sync target table schema with new_packages
            target_types = {
                r[1]: r[2]
                for r in self.conn.execute(
                    f"PRAGMA table_info('{table_name}')"
                ).fetchall()
            }
            for c in new_packages.column_names:
                src_type = self._arrow_to_duckdb(new_packages.schema.field(c).type)
                if c not in target_types:
                    self.conn.execute(
                        f'ALTER TABLE {table_name} ADD COLUMN "{c}" {src_type}'
                    )
                    target_types[c] = src_type
                elif target_types[c] != src_type and src_type == "VARCHAR":
                    # Widen the column type — DuckDB allows promotion to VARCHAR
                    self.conn.execute(
                        f'ALTER TABLE {table_name} ALTER "{c}" TYPE VARCHAR'
                    )
                    target_types[c] = "VARCHAR"

            cols = ", ".join(f'"{c}"' for c in new_packages.column_names)
            cast_cols = ", ".join(
                f'CAST("{c}" AS {target_types[c]})' for c in new_packages.column_names
            )
            total_inserted = (
                self.conn.execute(f"""
                        INSERT INTO {table_name} ({cols})
                        SELECT {cast_cols} FROM new_packages
                        ANTI JOIN {table_name}
                        USING (id)
                    """)
                .arrow()
                .read_all()["Count"][0]
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
        """Merge schemas, promoting conflicting types to string."""
        new_fields = {f.name: f for f in new_packages.schema}
        cur_fields = {f.name: f for f in current_packages.schema}
        all_names = list(dict.fromkeys(list(new_fields) + list(cur_fields)))

        merged_fields = []
        for name in all_names:
            nf = new_fields.get(name)
            cf = cur_fields.get(name)
            if nf and cf:
                if nf.type == cf.type:
                    merged_fields.append(nf)
                else:
                    merged_fields.append(pyarrow.field(name, pyarrow.string()))
            elif nf:
                merged_fields.append(nf)
            else:
                merged_fields.append(cf)

        merged_schema = pyarrow.schema(merged_fields)

        missing_fields = [
            f for f in merged_schema if f.name not in new_packages.column_names
        ]
        for field in missing_fields:
            new_packages = new_packages.append_column(
                field, pyarrow.nulls(new_packages.num_rows, field.type)
            )
        return new_packages.select([f.name for f in merged_schema]).cast(merged_schema)

    @staticmethod
    def _arrow_to_duckdb(arrow_type) -> str:
        if pyarrow.types.is_string(arrow_type) or pyarrow.types.is_large_string(
            arrow_type
        ):
            return "VARCHAR"
        if pyarrow.types.is_int64(arrow_type):
            return "BIGINT"
        if pyarrow.types.is_int32(arrow_type):
            return "INTEGER"
        if pyarrow.types.is_float64(arrow_type):
            return "DOUBLE"
        if pyarrow.types.is_boolean(arrow_type):
            return "BOOLEAN"
        if pyarrow.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        if pyarrow.types.is_date(arrow_type):
            return "DATE"
        if pyarrow.types.is_list(arrow_type) or pyarrow.types.is_struct(arrow_type):
            return "JSON"
        return "VARCHAR"

    def get_outdated_resources_id(self, ckan_url: str = "") -> List[str]:
        query = """
            SELECT id, last_modified FROM ckan_resource
            ANTI JOIN ckan_resource_last_update
              ON id = ckan_resource_id
              AND ckan_resource.last_modified::TIMESTAMP < ckan_resource_last_update.last_modified
        """
        params: list = []
        if ckan_url:
            query += " WHERE ckan_resource.ckan_url = ?"
            params.append(ckan_url)
        rows = self.conn.execute(query, params).fetchall()

        return list(map(lambda row: row[0], rows))
