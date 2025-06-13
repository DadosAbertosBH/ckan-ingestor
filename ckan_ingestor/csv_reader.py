from logging import exception

import duckdb
import pyarrow


class DuckDbCsvReader:

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def read(self, url: str) -> pyarrow.Table:
        for encoding in ["utf-8", "latin-1", "CWI"]:
            try:
                return self.conn.execute(f"SELECT * FROM read_csv('{url}', sample_size=-1, encoding='{encoding}')").arrow()
            except duckdb.InvalidInputException as e:
                last_e = e
        raise last_e
