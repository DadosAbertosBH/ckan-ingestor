import uuid
from contextlib import contextmanager
from typing import Generator
from xml.dom.minidom import Document

import dagster as dg
import duckdb
from dagster import InitResourceContext
from dagster._config.pythonic_config.conversion_utils import TResValue

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.csv_reader import DuckDbCsvReader
from ckan_ingestor.datastore_reader import DatastoreReader
from ckan_ingestor.duckdb_ckan_data_ingestor import DuckdbCkanDataIngestor
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor
from ckan_ingestor.s3_pdf_ingestor import S3DocumentIngestor


class DuckdbDataIngestorResource(dg.ConfigurableResource[DuckdbCkanDataIngestor]):
    metadata_ingestor: dg.ResourceDependency[DuckdbCkanMetadataIngestor]
    ducklake_settings: dg.ResourceDependency[DucklakeSettings]

    #
    # def create_resource(self, context: dg.InitResourceContext):
    #     connection_config = {"memory_limit": "1GB", 'threads': 1}
    #     with duckdb.connect(f":memory:{uuid.uuid4()}", config=connection_config) as local_conn:
    #         return DuckdbCkanDataIngestor(
    #             lock=self.metadata_ingestor.lock,
    #             ducklake_conn=self.metadata_ingestor.conn,
    #             document_ingestor=S3DocumentIngestor(s3_settings=self.ducklake_settings.data_path),
    #             datastore_reader=DatastoreReader(conn=local_conn,
    #                                              dastore_url=self.ducklake_settings.datastore_url),
    #             csv_reader=DuckDbCsvReader(conn=local_conn)
    #         )

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        connection_config = {"memory_limit": "1GB", 'threads': 1}
        with duckdb.connect(f":memory:{uuid.uuid4()}", config=connection_config) as local_conn:
            yield DuckdbCkanDataIngestor(
                lock=self.metadata_ingestor.lock,
                ducklake_conn=self.metadata_ingestor.conn,
                document_ingestor=S3DocumentIngestor(s3_settings=self.ducklake_settings.data_path),
                datastore_reader=DatastoreReader(conn=local_conn,
                                                 dastore_url=self.ducklake_settings.datastore_url),
                csv_reader=DuckDbCsvReader(conn=local_conn)
            )
