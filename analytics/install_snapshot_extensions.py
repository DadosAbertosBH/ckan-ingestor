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
"""Bootstrap signed DuckDB extensions over HTTPS for the snapshot image."""

import gzip
import os
import tempfile
import urllib.request
from pathlib import Path

import duckdb


def main():
    with duckdb.connect(
        config={"extension_directory": os.environ["DUCKDB_EXTENSION_DIRECTORY"]}
    ) as conn:
        platform = conn.sql("PRAGMA platform").fetchone()[0]
        version = duckdb.__version__
        with tempfile.TemporaryDirectory(prefix="duckdb-extensions-") as work:
            for name in ("httpfs", "postgres_scanner", "ducklake"):
                url = f"https://extensions.duckdb.org/v{version}/{platform}/{name}.duckdb_extension.gz"
                request = urllib.request.Request(
                    url, headers={"User-Agent": f"DuckDB/v{version}"}
                )
                with urllib.request.urlopen(request, timeout=60) as response:
                    binary = gzip.decompress(response.read())
                path = Path(work) / f"{name}.duckdb_extension"
                path.write_bytes(binary)
                conn.execute(f"INSTALL '{path}'")
                # Loading checks the official extension signature and engine ABI.
                conn.execute(f"LOAD {name}")


if __name__ == "__main__":
    main()
