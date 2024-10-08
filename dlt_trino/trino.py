from typing import Iterable, Optional

from dlt import Schema
from dlt.common.destination import DestinationCapabilitiesContext, PreparedTableSchema
from dlt.common.destination.reference import SupportsStagingDestination
from dlt.common.schema.typing import TColumnType, TColumnSchema
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
from trino.exceptions import Error

from dlt_trino.configuration import TrinoClientConfiguration
from dlt_trino.sql_client import TrinoSQLClient


class TrinoClient(SqlJobClientWithStagingDataset, SupportsStagingDestination):

    def __init__(
            self,
            schema: Schema,
            config: TrinoClientConfiguration,
            capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = TrinoSQLClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.sql_client: TrinoClient = sql_client  # type: ignore
        self.config: TrinoClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # only truncate tables in iceberg mode
        truncate_tables = []
        super().initialize_storage(truncate_tables)

    def _from_db_type(
            self, hive_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(hive_t, precision, scale)

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        return (
            f"{self.sql_client.escape_ddl_identifier(c['name'])} {self.type_mapper.to_destination_type(c, table)}"
        )

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, Error)
