import duckdb
import pyarrow
import pyarrow.csv as csv

class DuckDbCsvReader:

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def read(self, url: str) -> pyarrow.Table:
        for encoding in ["utf-8", "latin-1", "CWI"]:
            try:
                return self.conn.execute(f"SELECT * FROM read_csv('{url}', sample_size=-1, encoding='{encoding}')").arrow()
            except duckdb.InvalidInputException:
                pass
        return csv.read_csv(url)
