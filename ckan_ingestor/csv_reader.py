import io

import duckdb
import pyarrow
import pyarrow.csv as csv
import requests


class DuckDbCsvReader:

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def read(self, url: str) -> pyarrow.Table:
        for encoding in ["utf-8", "latin-1", "CWI"]:
            try:
                return self.conn.execute(f"SELECT * FROM read_csv('{url}', sample_size=-1, encoding='{encoding}')").arrow()
            except duckdb.InvalidInputException:
                pass

        with requests.get(url, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
        }) as response:
            response.raise_for_status()
            for delimiter in [",", ";"]:
                try:
                    table = csv.read_csv(io.BytesIO(response.content), parse_options=csv.ParseOptions(delimiter=delimiter))
                    if table.num_rows > 0:
                        return table
                except pyarrow.lib.ArrowInvalid:
                    pass

            raise duckdb.IOException(f"Failed to parse CSV file from {url}")

