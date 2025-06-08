import time

import duckdb
import pyarrow as pa

from ckan_ingestor.config.ducklake_settings import DucklakeSettings

CKAN_DATASET_TABLE = "ckan_dataset"

class DuckdbCkanIngestor:
    packages: pa.Table
    conn: duckdb

    def __init__(self, dataset: pa.Table):
        self.settings = DucklakeSettings()
        self.packages = dataset
        self.conn = self._connect()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Return a duckdb connection with required extensions."""
        conn = duckdb.connect(self.settings.database)
        conn.install_extension("ducklake")
        conn.load_extension("ducklake")
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("SET pg_debug_show_queries=false;")


        account_id = "" if self.settings.data_path.account_id is None \
            else f",'ACCOUNT_ID '{self.settings.data_path.account_id}'"

        stmt = f"""
                CREATE OR REPLACE SECRET secret (
                    TYPE '{self.settings.data_path.protocol}',
                    ENDPOINT '{self.settings.data_path.endpoint}',
                    KEY_ID '{self.settings.data_path.access_key_id}',
                    SECRET '{self.settings.data_path.secret_access_key}',
                    USE_SSL '{self.settings.data_path.use_ssl}',
                    URL_STYLE '{self.settings.data_path.url_style}'
                    {account_id}
                );
            """

        conn.execute(stmt)

        stmt = (
            "ATTACH 'ducklake:{conn}' AS lake (DATA_PATH '{data_path_protocol}://{data_path_bucket}');"

        ).format(
            conn=self.settings.catalog_uri,
            data_path_protocol=self.settings.data_path.protocol,
            data_path_bucket=self.settings.data_path.bucket,
        )

        conn.execute(stmt)
        conn.execute("USE lake;")
        return conn

    def table_exists(self, table_name):
        try:
            result = self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return len(result) > 0
        except duckdb.CatalogException:
            return False

    def ingest(self):
        resources = self.packages["resources"].combine_chunks().flatten()
        # noinspection PyArgumentList
        _tables = pa.Table.from_struct_array(resources)
        new_packages = self.packages.drop_columns("resources")
        self.conn.execute("BEGIN TRANSACTION;")
        if self.table_exists(CKAN_DATASET_TABLE):
            start_time = time.time()
            print("New dataset size:", new_packages.num_rows)

            deleted_count = self.conn.execute(f"""
                    DELETE FROM {CKAN_DATASET_TABLE}
                    WHERE id IN (SELECT new_packages.id FROM new_packages
                        ASOF JOIN {CKAN_DATASET_TABLE} current_packages
                        ON (new_packages.id = current_packages.id AND 
                            new_packages.metadata_modified > current_packages.metadata_modified)
                    )
                """).arrow()["Count"][0].as_py()
            print("Datasets to update:", deleted_count)

            total_inserted = self.conn.execute(f"""
                    INSERT INTO {CKAN_DATASET_TABLE}
                    SELECT * from new_packages
                    ANTI JOIN {CKAN_DATASET_TABLE}
                    USING (id)
                """).arrow()["Count"][0].as_py()
            print("New datasets to inserted:", total_inserted - deleted_count)

            print("---Merged dataset in %s seconds ---" % (time.time() - start_time))
        else:
            self.conn.execute(f"""
                    CREATE TABLE {CKAN_DATASET_TABLE} AS select * from new_packages
                """)
        self.conn.execute("COMMIT;")