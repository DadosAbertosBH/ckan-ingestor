import asyncio
from datetime import datetime

import duckdb
import pyarrow
import pyarrow as pa
import pytz
from minio import Minio

from ckan_ingestor.config.ducklake_settings import DucklakeSettings

CKAN_DATASET_TABLE = "ckan_dataset"
CKAN_RESOURCE_TABLE = "ckan_resource"


class DuckdbCkanIngestor:
    packages: pa.Table
    conn: duckdb

    def __init__(self, dataset: pa.Table, datastore_url='https://dados.pbh.gov.br/datastore/dump/'):
        self.settings = DucklakeSettings()
        self.datastore_url = datastore_url
        self.packages = dataset
        self.minio = Minio(
            "your-minio-endpoint:9000",  # Replace with your MinIO server address
            access_key="your-access-key",  # Replace with your access key
            secret_key="your-secret-key",  # Replace with your secret key
            secure=False,  # Set to True if using HTTPS
        )
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
        ckan_resources = pa.Table.from_struct_array(resources)
        ckan_datasets = self.packages.drop_columns("resources")
        self.conn.execute("BEGIN TRANSACTION;")
        self.merge_dataset(ckan_datasets, CKAN_DATASET_TABLE, "metadata_modified")
        self.merge_dataset(ckan_resources, CKAN_RESOURCE_TABLE, "last_modified")

        self.ingest_ckan_data_async(ckan_resources)
        # asyncio.run(self.ingest_ckan_data_async(ckan_resources))

        self.conn.execute("COMMIT;")

    def merge_dataset(self, new_packages: pyarrow.Table, table_name: str, update_at_column: str):
        if self.table_exists(table_name):
            print(f"{table_name} new dataset size:", new_packages.num_rows)
            deleted_count = self.conn.execute(f"""
                        DELETE FROM {table_name}
                        WHERE id IN (SELECT new_packages.id FROM new_packages
                            ASOF JOIN {table_name} current_packages
                            ON (new_packages.id = current_packages.id AND 
                                new_packages.{update_at_column} > current_packages.{update_at_column})
                        )
                    """).arrow()["Count"][0].as_py()
            print(f"{table_name} rows to deleted:", deleted_count)
            total_inserted = self.conn.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * from new_packages
                        ANTI JOIN {table_name}
                        USING (id)
                    """).arrow()["Count"][0].as_py()
            print(f"{table_name} rows inserted:", total_inserted)

        else:
            print(f"{table_name} created")
            self.conn.execute(f"""
                    CREATE TABLE {table_name} AS select * from new_packages
                """)

    def table_is_up_to_date(self, table_name: str, last_modified: str) -> bool:
        """Check if the table is up to date based on the last modified timestamp."""
        if not self.table_exists(table_name):
            return False

        snapshot_time = self.conn.execute(f"""
            SELECT snapshot_time FROM snapshots()
            WHERE snapshot_id in (
                SELECT snapshot_id FROM table_changes('{table_name}', now(), now())
            )
        """).fetchone()[0]

        return snapshot_time > pytz.UTC.localize(datetime.fromisoformat(last_modified))

    def ingest_ckan_data_async(self, ckan_resources: pyarrow.Table):
        for r in ckan_resources.to_pylist():
            self.ingest_ckan_data(r)
        # tasks = [asyncio.create_task(self.ingest_ckan_data(r)) for r in ckan_resources.to_pylist()]
        # await  asyncio.gather(*tasks)

    def ingest_ckan_data(self, ckan_resource: dict[str: any], attempt_formats=None):
        if attempt_formats is None:
            attempt_formats = []
        # print(f"Working on {ckan_resource['id']}")

        if ckan_resource["datastore_active"] and "JSON" not in attempt_formats:
            url = f"{self.datastore_url}/{ckan_resource['id']}?format=json"
            attempt_formats.append("JSON")
            read_function = f"read_json('{url}', maximum_object_size=2_147_483_648)"
        elif ckan_resource["format"] == "CSV" and "CSV" not in attempt_formats:
            attempt_formats.append("CSV")
            read_function = f"read_csv('{ckan_resource['url']}', sample_size=-1)"
        else:
            print(f"Resource {ckan_resource['id']} from resource {ckan_resource['name']} "
                  f"have a unsupported format {ckan_resource['format']}")
            return

        if not self.table_is_up_to_date(ckan_resource["id"], ckan_resource["last_modified"]):
            print(f"updating {ckan_resource['id']} from resource {ckan_resource['name']}")
            try:
                self.conn.execute(f'CREATE OR REPLACE TABLE "{ckan_resource["id"]}" AS SELECT * FROM {read_function}')
            except duckdb.InvalidInputException as e:
                print(
                    f"Failed to parser {ckan_resource['id']} from resource {ckan_resource['name']} "
                    f"using formats {attempt_formats}")
                self.ingest_ckan_data(ckan_resource, attempt_formats)
        else:
            print(f"Table {ckan_resource['id']} from resource {ckan_resource['name']} is up to date")
        # print(f"Finished working on {ckan_resource['id']}")
