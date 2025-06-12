import duckdb
import pyarrow


class DuckDbCsvReader:

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def read(self, url: str) -> pyarrow.Table:
        try:
            return self.conn.execute(f"SELECT * FROM read_csv('{url}', sample_size=-1)").arrow()
        except duckdb.InvalidInputException as e:
            if "Invalid unicode" in str(e):
                return self.conn.execute(f"SELECT * FROM read_csv('{url}', sample_size=-1, encoding='latin-1')").arrow()
            raise e
