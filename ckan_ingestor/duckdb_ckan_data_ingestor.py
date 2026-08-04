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

import duckdb
import requests

from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


class DuckdbCkanDataIngestor:
    ducklake_conn: duckdb
    document_ingestor: S3DocumentIngestor
    csv_reader: DuckDbCsvReader
    datastore_reader: DatastoreReader
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        ducklake_conn: duckdb.DuckDBPyConnection,
        document_ingestor: S3DocumentIngestor,
        datastore_reader: DatastoreReader,
        csv_reader: DuckDbCsvReader,
    ):
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.ducklake_conn = ducklake_conn
        self.document_ingestor = document_ingestor
        self.datastore_reader = datastore_reader
        self.csv_reader = csv_reader

    def ingest_ckan_data(
        self, ckan_resource: dict[str:any], attempt_formats=None
    ) -> bool:
        # thread_name = str(current_thread().name)
        # print(f"threads write_thread_{thread_name}")
        resource_id = ckan_resource["id"]
        if attempt_formats is None:
            attempt_formats = []

        try:
            self.logger.debug(
                f"updating {ckan_resource['id']} from resource {ckan_resource['name']}"
            )
            if (
                ckan_resource["datastore_active"]
                and "DATA_STORE" not in attempt_formats
            ):
                attempt_formats.append("DATA_STORE")
                _datastore_table = self.datastore_reader.read(resource_id)
                if _datastore_table is None:
                    self.logger.info(
                        f"Resource {resource_id} datastore is empty,"
                        " falling back to file-based formats"
                    )
                    return self.ingest_ckan_data(ckan_resource, attempt_formats)
                query = "SELECT * FROM _datastore_table"
            elif ckan_resource["format"] == "CSV" and "CSV" not in attempt_formats:
                attempt_formats.append("CSV")
                _csv_table = self.csv_reader.read(ckan_resource["url"])
                query = "SELECT * FROM _csv_table"
            elif ckan_resource["format"] == "JSON" and "JSON" not in attempt_formats:
                attempt_formats.append("JSON")
                query = f"SELECT * FROM read_json('{ckan_resource['url']}', maximum_object_size=268435456)"
            elif ckan_resource["format"] == "PDF" and "PDF" not in attempt_formats:
                attempt_formats.append("PDF")
                download_url = self.document_ingestor.ingest(
                    filename=f"{resource_id}.pdf",
                    download_url=ckan_resource["url"],
                    content_type="application/pdf",
                )
                query = f"SELECT '{download_url}' as url"
            elif ckan_resource["format"] == "DOCX" and "DOCX" not in attempt_formats:
                attempt_formats.append("DOCX")
                download_url = self.document_ingestor.ingest(
                    filename=f"{resource_id}.docx",
                    download_url=ckan_resource["url"],
                    content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
                query = f"SELECT '{download_url}' as url"
            else:
                self.logger.error(
                    f"Resource {resource_id} from resource {ckan_resource['name']} "
                    f"have a unsupported format {ckan_resource['format']}"
                )
                return False
            self.ducklake_conn.execute(
                f'CREATE OR REPLACE TABLE "{resource_id}" AS {query}'
            )
            self.ducklake_conn.execute(
                "DELETE FROM ckan_resource_last_update where ckan_resource_id = ?",
                (resource_id,),
            )
            self.ducklake_conn.execute(
                """
                INSERT INTO ckan_resource_last_update (ckan_resource_id, last_modified) VALUES (?, NOW())
                """,
                (resource_id,),
            )
        except (
            duckdb.InvalidInputException,
            duckdb.IOException,
            requests.exceptions.JSONDecodeError,
            requests.exceptions.HTTPError,
        ) as e:
            self.logger.warning(
                f"Failed to parser {resource_id} from resource {ckan_resource['name']} error = {e}"
                f" using formats {attempt_formats}"
            )
            return self.ingest_ckan_data(ckan_resource, attempt_formats)

        self.logger.info(f"Finished working on {ckan_resource['id']}")
        return True
