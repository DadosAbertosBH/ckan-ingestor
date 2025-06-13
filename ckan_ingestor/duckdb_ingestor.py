import logging
from datetime import datetime

import duckdb
import pyarrow
import pyarrow as pa
import pytz
import requests

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_connection_factory import from_settings
from ckan_ingestor.s3_pdf_ingestor import S3PdfIngestor

CKAN_DATASET_TABLE = "ckan_dataset"
CKAN_RESOURCE_TABLE = "ckan_resource"


class DuckdbCkanIngestor:
    conn: duckdb
    pdf_ingestor: S3PdfIngestor
    csv_reader: DuckDbCsvReader
    datastore_reader: DatastoreReader
    logger = logging.getLogger(__name__)

    def __init__(
            self,
            conn: duckdb.DuckDBPyConnection,
            pdf_ingestor: S3PdfIngestor,
            datastore_url: str,
    ):
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.datastore_reader = DatastoreReader(conn=conn, dastore_url=datastore_url)
        self.pdf_ingestor = pdf_ingestor
        self.csv_reader = DuckDbCsvReader(conn)
        self.conn = conn

    @classmethod
    def from_settings(
            cls,
            datastore_url='https://dados.pbh.gov.br/datastore/dump/',
            settings: DucklakeSettings = DucklakeSettings()
    ):
        return cls(
            datastore_url=datastore_url,
            conn=from_settings(settings=settings),
            pdf_ingestor=S3PdfIngestor(settings.data_path)
        )

    def table_exists(self, table_name):
        try:
            result = self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return len(result) > 0
        except duckdb.CatalogException:
            return False

    def ingest(self, packages: pyarrow.Table):
        resources = packages["resources"].combine_chunks().flatten()
        # noinspection PyArgumentList
        ckan_resources = pa.Table.from_struct_array(resources)
        ckan_datasets = packages.drop_columns("resources")
        self.conn.begin()
        self.merge_dataset(ckan_datasets, CKAN_DATASET_TABLE, "metadata_modified")
        self.merge_dataset(ckan_resources, CKAN_RESOURCE_TABLE, "last_modified")
        self.conn.commit()

        self.ingest_ckan_data_async(ckan_resources)
        # asyncio.run(self.ingest_ckan_data_async(ckan_resources))

    def merge_dataset(self, new_packages: pyarrow.Table, table_name: str, update_at_column: str):
        if self.table_exists(table_name):
            current_packages = self.conn.table(table_name).arrow()
            new_packages = self._merge_schema(new_packages, current_packages)
            self.logger.info(f"{table_name} new dataset size: {new_packages.num_rows}")
            deleted_count = self.conn.execute(f"""
                        DELETE FROM {table_name}
                        WHERE id IN (SELECT new_packages.id FROM new_packages
                            ASOF JOIN current_packages
                            ON (new_packages.id = current_packages.id AND 
                                new_packages.{update_at_column} > current_packages.{update_at_column})
                        )
                    """).arrow()["Count"][0].as_py()
            self.logger.info(f"{table_name} rows to deleted:{deleted_count}")
            total_inserted = self.conn.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * from new_packages
                        ANTI JOIN {table_name}
                        USING (id)
                    """).arrow()["Count"][0].as_py()
            self.logger.info(f"{table_name} rows inserted:{total_inserted}")

        else:
            self.logger.info(f"{table_name} created")
            self.conn.execute(f"""
                    CREATE TABLE {table_name} AS select * from new_packages
                """)

    def table_is_up_to_date(self, table_name: str, last_modified: str) -> bool:
        """Check if the table is up to date based on the last modified timestamp."""
        if not self.table_exists(table_name):
            return False

        max_snapshot = self.conn.execute(" SELECT MAX(snapshot_id) FROM  snapshots()").fetchone()[0]
        snapshot_time = self.conn.execute(f"""
            SELECT snapshot_time FROM snapshots()
            WHERE snapshot_id in (
                SELECT MAX(snapshot_id) FROM table_changes('{table_name}', 0, {max_snapshot})
            )
        """).fetchone()[0]

        return snapshot_time > pytz.UTC.localize(datetime.fromisoformat(last_modified))

    def ingest_ckan_data_async(self, ckan_resources: pyarrow.Table):
        for r in ckan_resources.to_pylist():
            self.ingest_ckan_data(r)
        # tasks = [asyncio.create_task(self.ingest_ckan_data(r)) for r in ckan_resources.to_pylist()]
        # await  asyncio.gather(*tasks)

    def ingest_ckan_data(self, ckan_resource: dict[str: any], attempt_formats=None):
        self.logger.info(f"Working on {ckan_resource['id']}")
        resource_id = ckan_resource['id']
        if attempt_formats is None:
            attempt_formats = []

        if not self.table_is_up_to_date(resource_id, ckan_resource["last_modified"]):
            try:
                self.logger.debug(f"updating {ckan_resource['id']} from resource {ckan_resource['name']}")
                if ckan_resource["datastore_active"] and "DATA_STORE" not in attempt_formats:
                    attempt_formats.append("DATA_STORE")
                    datastore_table = self.datastore_reader.read(resource_id)
                    self.conn.register("datastore_table", datastore_table)
                    query = f"SELECT * FROM datastore_table"
                elif ckan_resource["format"] == "CSV" and "CSV" not in attempt_formats:
                    attempt_formats.append("CSV")
                    csv_table = self.csv_reader.read(ckan_resource['url'])
                    self.conn.register("csv_table", csv_table)
                    query = f"SELECT * FROM csv_table"
                elif ckan_resource["format"] == "JSON" and "JSON" not in attempt_formats:
                    attempt_formats.append("JSON")
                    query = f"SELECT * FROM read_json('{ckan_resource['url']}', maximum_object_size=2_147_483_648)"
                elif ckan_resource["format"] == "PDF" and "PDF" not in attempt_formats:
                    attempt_formats.append("PDF")
                    download_url = self.pdf_ingestor.ingest(resource_id, ckan_resource['url'])
                    query = f"SELECT '{download_url}' as url"
                else:
                    self.logger.error(f"Resource {resource_id} from resource {ckan_resource['name']} "
                                      f"have a unsupported format {ckan_resource['format']}")
                    return
                self.conn.execute(f'CREATE OR REPLACE TABLE "{ckan_resource["id"]}" AS {query}')
            except (duckdb.InvalidInputException, duckdb.IOException, requests.exceptions.JSONDecodeError) as e:
                self.logger.warning(
                    f"Failed to parser {resource_id} from resource {ckan_resource['name']} error = {e}"
                    f"using formats {attempt_formats}")
                self.ingest_ckan_data(ckan_resource, attempt_formats)
        else:
            self.logger.debug(f"Table {ckan_resource['id']} from resource {ckan_resource['name']} is up to date")

        self.logger.debug(f"Finished working on {ckan_resource['id']}")

    @staticmethod
    def _merge_schema(new_packages, current_packages):
        merged_schema = pyarrow.unify_schemas(
            [new_packages.schema, current_packages.schema],
            promote_options="permissive"
        )
        missing_fields = [f for f in merged_schema if f.name not in new_packages.column_names]
        for field in missing_fields:
            new_packages = new_packages.append_column(field, pyarrow.nulls(new_packages.num_rows, field.type))
        return new_packages.cast(merged_schema)

