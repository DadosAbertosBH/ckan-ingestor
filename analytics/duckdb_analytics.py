# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

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
import pandas as pd


class DuckDBAnalytics:
    """A wrapper around DuckDB for analytics queries.

    Provides an in-memory DuckDB connection with convenience methods
    for querying data and registering DataFrames as tables.
    """

    def __init__(self, database: str = ":memory:"):
        """Initialize the analytics connection.

        Args:
            database: Path to a DuckDB database file, or ":memory:" for in-memory.
        """
        self.conn = duckdb.connect(database)

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame.

        Args:
            sql: The SQL query string to execute.

        Returns:
            A pandas DataFrame with the query results.
        """
        return self.conn.execute(sql).fetchdf()

    def register(self, name: str, df: pd.DataFrame) -> None:
        """Register a pandas DataFrame as a table in DuckDB.

        Args:
            name: The table name to register.
            df: The pandas DataFrame to register.
        """
        self.conn.register(name, df)
