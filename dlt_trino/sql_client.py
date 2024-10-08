from contextlib import contextmanager
from typing import ClassVar, Iterator, AnyStr, Any, Optional, Sequence

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import DatabaseTransientException
from dlt.destinations.sql_client import SqlClientBase, raise_open_connection_error, raise_database_error, \
    DBApiCursorImpl
from dlt.destinations.typing import DBApi, DBTransaction, DBApiCursor

import trino
from trino.dbapi import Connection, connect
from trino.exceptions import OperationalError

from dlt_trino.configuration import TrinoClientConfiguration


class TrinoSQLClient(SqlClientBase[Connection]):
    dbapi: ClassVar[DBApi] = trino.dbapi

    def __init__(
            self,
            dataset_name: str,
            staging_dataset_name: str,
            config: TrinoClientConfiguration,
            capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # noinspection PyTypeChecker
        super().__init__(None, dataset_name, staging_dataset_name, capabilities)
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
            catalog=self.config.credentials.database,
        )
        return self._conn

    def close_connection(self) -> None:
        self._conn.close()
        self._conn = None

    @property
    def native_connection(self) -> Connection:
        return self._conn

    def fully_qualified_ddl_dataset_name(self) -> str:
        return self.escape_ddl_identifier(self.dataset_name)

    def make_qualified_ddl_table_name(self, table_name: str) -> str:
        table_name = self.escape_ddl_identifier(table_name)
        return f"{self.fully_qualified_ddl_dataset_name()}.{table_name}"

    def create_dataset(self) -> None:
        # HIVE escaping for DDL
        self.execute_sql(f"CREATE DATABASE {self.fully_qualified_ddl_dataset_name()};")

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

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        # convert sql and params to PyFormat, as athena does not support anything else
        db_args = args if args else kwargs if kwargs else None
        curr = self._conn.cursor()
        try:
            curr.execute(query, db_args)
            yield DBApiCursorImpl(curr)  # type: ignore
            # catch key error only here, this will show up if we have a missing parameter
        except KeyError:
            raise DatabaseTransientException(OperationalError())
        finally:
            curr.close()
