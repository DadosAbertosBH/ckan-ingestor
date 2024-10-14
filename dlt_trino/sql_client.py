import re
from contextlib import contextmanager
from typing import ClassVar, Iterator, AnyStr, Any, Optional, Sequence, Tuple, Dict

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.destinations.exceptions import DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.sql_client import SqlClientBase, raise_open_connection_error, raise_database_error, \
    DBApiCursorImpl
from dlt.destinations.typing import DBApi, DBTransaction, DBApiCursor

import trino
from trino.dbapi import Connection, connect
from trino.exceptions import OperationalError, TrinoConnectionError, TrinoUserError

from dlt_trino.configuration import TrinoClientConfiguration


class TrinoSQLClient(SqlClientBase[Connection]):
    TRINO_SCHEMA_NOT_FOUND_ERROR_CODE = 45
    TRINO_TABLE_NOT_FOUND_ERROR_CODE = 46
    dbapi: ClassVar[DBApi] = trino.dbapi

    def __init__(
            self,
            catalog: str,
            staging_dataset_name: str,
            config: TrinoClientConfiguration,
            capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # noinspection PyTypeChecker
        super().__init__(None, catalog, staging_dataset_name, capabilities)
        # noinspection PyTypeChecker
        self._conn: Connection = None
        self.config = config
        self.credentials = config.credentials

    @raise_open_connection_error
    def open_connection(self) -> Connection:
        self._conn = connect(
            host=self.config.credentials.host,
            port=self.config.credentials.port,
            user=self.config.credentials.username,
            catalog=self.config.credentials.catalog,
        )
        return self._conn

    def close_connection(self) -> None:
        self._conn.close()
        self._conn = None

    @property
    def native_connection(self) -> Connection:
        return self._conn

    def drop_dataset(self) -> None:
        self.execute_sql(f"DROP DATABASE {self.fully_qualified_ddl_dataset_name()} CASCADE;")

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        statements = [
            f"DROP TABLE IF EXISTS {self.make_qualified_ddl_table_name(table)};" for table in tables
        ]
        self.execute_many(statements)

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        raise NotImplementedError("You cannot rollback Athena SQL statements.")

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        return ex

    def execute_sql(
            self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    def _handle_trino_user_error(self, error: Any):
        match error:
            case TrinoUserError(error_code=self.TRINO_SCHEMA_NOT_FOUND_ERROR_CODE):
                return DestinationUndefinedEntity(error)
            case TrinoUserError(error_code=self.TRINO_TABLE_NOT_FOUND_ERROR_CODE):
                return DatabaseUndefinedRelation(error)
            case _:
                return DatabaseTransientException(error)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        # convert sql and params to PyFormat, as athena does not support anything else
        curr = self._conn.cursor()
        db_args = args if args else kwargs if kwargs else None
        try:
            new_query = query.replace("%s", "?").removesuffix(';')
            curr.execute(new_query, db_args)
            yield DBApiCursorImpl(curr)  # type: ignore
            # catch key error only here, this will show up if we have a missing parameter
        except trino.exceptions.Error as error:
            raise self._handle_trino_user_error(error)
        finally:
            curr.close()
