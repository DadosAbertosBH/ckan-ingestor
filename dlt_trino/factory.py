import typing as t

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext, PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TColumnSchema
from dlt.common.schema.typing import TColumnType
from dlt.common.typing import TLoaderFileFormat
from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.destinations.type_mapping import TypeMapperImpl

from dlt_trino.configuration import TrinoClientConfiguration, TrinoCredentials

from dlt_trino.trino import TrinoClient


class TrinoTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "varchar",
        "text": "varchar",
        "double": "double",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "bigint": "bigint",
        "binary": "VARBINARY",
        "time": "TIME(6)",
    }

    sct_to_dbt = {"decimal": "decimal(%i,%i)", "wei": "decimal(%i,%i)"}

    dbt_to_sct = {
        "varchar": "text",
        "double": "double",
        "boolean": "bool",
        "date": "date",
        "timestamp": "timestamp",
        "bigint": "bigint",
        "binary": "binary",
        "varbinary": "binary",
        "decimal": "decimal",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
    }

    def ensure_supported_type(
            self,
            column: TColumnSchema,
            table: PreparedTableSchema,
            loader_file_format: TLoaderFileFormat,
    ) -> None:
        # TIME is not supported for parquet on Athena
        if loader_file_format == "parquet" and column["data_type"] == "time":
            raise TerminalValueError(
                "Please convert `datetime.time` objects in your data to `str` or"
                " `datetime.datetime`.",
                "time",
            )

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "bigint"
        if precision <= 32:
            return "INTEGER"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into athena integer type"
        )

    def from_destination_type(
            self, db_type: str, precision: t.Optional[int], scale: t.Optional[int]
    ) -> TColumnType:
        for key, val in self.dbt_to_sct.items():
            if db_type.startswith(key):
                return without_none(dict(data_type=val, precision=precision, scale=scale))  # type: ignore[return-value]
        return dict(data_type=None)


class trino(Destination[TrinoClientConfiguration, "TrinoClient"]):
    spec = TrinoClientConfiguration

    def __init__(
            self,
            credentials: t.Union[TrinoCredentials, t.Dict[str, t.Any], str] = None,
            destination_name: t.Optional[str] = None,
            environment: t.Optional[str] = None,
            **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @property
    def client_class(self) -> t.Type["TrinoClient"]:
        return TrinoClient

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.schema_supports_numeric_precision = False
        caps.type_mapper = TrinoTypeMapper
        caps.supports_multiple_statements = False

        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.escape_identifier = escape_postgres_identifier
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = True
        caps.escape_literal = escape_postgres_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (2 * EVM_DECIMAL_PRECISION, EVM_DECIMAL_PRECISION)
        caps.max_identifier_length = 63
        caps.max_column_identifier_length = 63
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]

        return caps
