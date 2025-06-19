import logging
import threading

import duckdb
import requests

from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


class DuckdbCkanDataIngestor:
    conn: duckdb
    document_ingestor: S3DocumentIngestor
    csv_reader: DuckDbCsvReader
    datastore_reader: DatastoreReader
    logger = logging.getLogger(__name__)

    def __init__(
            self,
            lock: threading.RLock,
            ducklake_conn: duckdb.DuckDBPyConnection,
            document_ingestor: S3DocumentIngestor,
            datastore_reader: DatastoreReader,
            csv_reader: DuckDbCsvReader,
    ):
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.lock = lock
        self.ducklake_conn = ducklake_conn
        self.document_ingestor = document_ingestor
        self.datastore_reader = datastore_reader
        self.csv_reader = csv_reader

    def ingest_ckan_data(self, ckan_resource: dict[str: any], attempt_formats=None):
        # thread_name = str(current_thread().name)
        # print(f"threads write_thread_{thread_name}")
        resource_id = ckan_resource['id']
        if attempt_formats is None:
            attempt_formats = []

        try:
            self.logger.debug(f"updating {ckan_resource['id']} from resource {ckan_resource['name']}")
            if ckan_resource["datastore_active"] and "DATA_STORE" not in attempt_formats:
                attempt_formats.append("DATA_STORE")
                _datastore_table = self.datastore_reader.read(resource_id)
                query = f"SELECT * FROM _datastore_table"
            elif ckan_resource["format"] == "CSV" and "CSV" not in attempt_formats:
                attempt_formats.append("CSV")
                _csv_table = self.csv_reader.read(ckan_resource['url'])
                query = f"SELECT * FROM _csv_table"
            elif ckan_resource["format"] == "JSON" and "JSON" not in attempt_formats:
                attempt_formats.append("JSON")
                query = f"SELECT * FROM read_json('{ckan_resource['url']}', maximum_object_size=2_147_483_648)"
            elif ckan_resource["format"] == "PDF" and "PDF" not in attempt_formats:
                attempt_formats.append("PDF")
                download_url = self.document_ingestor.ingest(
                    filename=f"{resource_id}.pdf",
                    download_url=ckan_resource['url'],
                    content_type="application/pdf"
                )
                query = f"SELECT '{download_url}' as url"
            elif ckan_resource["format"] == "DOCX" and "DOCX" not in attempt_formats:
                attempt_formats.append("DOCX")
                download_url = self.document_ingestor.ingest(
                    filename=f"{resource_id}.docx",
                    download_url=ckan_resource['url'],
                    content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                )
                query = f"SELECT '{download_url}' as url"
            else:
                self.logger.error(f"Resource {resource_id} from resource {ckan_resource['name']} "
                                  f"have a unsupported format {ckan_resource['format']}")
                return
            with self.lock:
                self.ducklake_conn.execute(f'CREATE OR REPLACE TABLE "{ckan_resource["id"]}" AS {query}')
        except (duckdb.InvalidInputException, duckdb.IOException, requests.exceptions.JSONDecodeError,
                requests.exceptions.HTTPError) as e:
            self.logger.warning(
                f"Failed to parser {resource_id} from resource {ckan_resource['name']} error = {e}"
                f" using formats {attempt_formats}")
            self.ingest_ckan_data(ckan_resource, attempt_formats)

        self.logger.info(f"Finished working on {ckan_resource['id']}")

