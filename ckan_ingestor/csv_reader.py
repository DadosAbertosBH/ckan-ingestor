# Pedalin
# Copyright (C) 2025  Pedalin

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
import io

import duckdb
import pyarrow
import pyarrow.csv as csv
import requests


class DuckDbCsvReader:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.last_encoding: str | None = None

    def read(self, url: str) -> pyarrow.Table:
        for encoding in ["utf-8", "latin-1", "utf-16"]:
            try:
                result = (
                    self.conn.execute(
                        f"SELECT * FROM read_csv('{url}', sample_size=100000, encoding='{encoding}')"
                    )
                    .arrow()
                    .read_all()
                )
                self.last_encoding = encoding
                return result
            except duckdb.InvalidInputException:
                pass

        with requests.get(
            url,
            stream=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
            },
        ) as response:
            response.raise_for_status()
            for delimiter in [",", ";"]:
                try:
                    table = csv.read_csv(
                        io.BytesIO(response.content),
                        parse_options=csv.ParseOptions(delimiter=delimiter),
                    )
                    if table.num_rows > 0:
                        return table
                except pyarrow.lib.ArrowInvalid:
                    pass

            raise duckdb.IOException(f"Failed to parse CSV file from {url}")
