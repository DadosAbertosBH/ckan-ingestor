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
import duckdb
import pyarrow

from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


class ResourceReader:
    def __init__(
        self,
        datastore_url: str,
        document_ingestor: S3DocumentIngestor | None = None,
    ):
        self.datastore_reader = DatastoreReader(datastore_url)
        self.document_ingestor = document_ingestor

    def read(self, ckan_resource: dict, attempt_formats=None) -> pyarrow.Table | None:
        resource_id = ckan_resource["id"]
        if attempt_formats is None:
            attempt_formats = []

        try:
            if (
                ckan_resource["datastore_active"]
                and "DATA_STORE" not in attempt_formats
            ):
                attempt_formats.append("DATA_STORE")
                return self.datastore_reader.read(resource_id)
            elif ckan_resource["format"] == "CSV" and "CSV" not in attempt_formats:
                attempt_formats.append("CSV")
                return self._read_csv(ckan_resource["url"])
            elif ckan_resource["format"] == "JSON" and "JSON" not in attempt_formats:
                attempt_formats.append("JSON")
                return self._read_json(ckan_resource["url"])
            elif (
                self.document_ingestor
                and ckan_resource["format"] == "PDF"
                and "PDF" not in attempt_formats
            ):
                attempt_formats.append("PDF")
                return self._read_document(ckan_resource, "pdf", "application/pdf")
            elif (
                self.document_ingestor
                and ckan_resource["format"] == "DOCX"
                and "DOCX" not in attempt_formats
            ):
                attempt_formats.append("DOCX")
                return self._read_document(
                    ckan_resource,
                    "docx",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
            else:
                return None
        except Exception:
            return self.read(ckan_resource, attempt_formats)

    def _read_csv(self, url: str) -> pyarrow.Table:
        with duckdb.connect(":memory:") as conn:
            reader = DuckDbCsvReader(conn)
            return reader.read(url)

    def _read_json(self, url: str) -> pyarrow.Table:
        with duckdb.connect(":memory:") as conn:
            return (
                conn.execute(
                    f"SELECT * FROM read_json('{url}', maximum_object_size=268435456)"
                )
                .arrow()
                .read_all()
            )

    def _read_document(
        self, ckan_resource: dict, ext: str, content_type: str
    ) -> pyarrow.Table:
        resource_id = ckan_resource["id"]
        download_url = self.document_ingestor.ingest(
            filename=f"{resource_id}.{ext}",
            download_url=ckan_resource["url"],
            content_type=content_type,
        )
        return pyarrow.table({"url": [download_url]})
