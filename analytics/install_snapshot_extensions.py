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

import argparse
import gzip
import os
import urllib.request
from pathlib import Path


EXTENSIONS = ("httpfs", "postgres_scanner", "ducklake")


def download_extensions(directory: Path, *, version: str, platform: str):
    """Place official extension binaries where DuckDB resolves them at runtime.

    This deliberately does not import DuckDB so a multi-architecture Docker
    build can fetch the target platform's extensions without running its
    native wheel through QEMU.
    """
    target = Path(directory) / f"v{version}" / platform
    target.mkdir(parents=True, exist_ok=True)
    for name in EXTENSIONS:
        url = f"https://extensions.duckdb.org/v{version}/{platform}/{name}.duckdb_extension.gz"
        request = urllib.request.Request(
            url, headers={"User-Agent": f"DuckDB/v{version}"}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            binary = gzip.decompress(response.read())
        (target / f"{name}.duckdb_extension").write_bytes(binary)


def duckdb_runtime():
    import duckdb

    return duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version")
    parser.add_argument("--platform")
    parser.add_argument("--skip-verify", action="store_true")
    args = parser.parse_args()
    directory = Path(os.environ["DUCKDB_EXTENSION_DIRECTORY"])
    duckdb = None
    if not (args.version and args.platform and args.skip_verify):
        duckdb = duckdb_runtime()
    if args.platform:
        platform = args.platform
    else:
        assert duckdb is not None
        with duckdb.connect(config={"extension_directory": str(directory)}) as conn:
            platform = conn.sql("PRAGMA platform").fetchone()[0]
    version = args.version or duckdb.__version__
    download_extensions(directory, version=version, platform=platform)
    if not args.skip_verify:
        assert duckdb is not None
        with duckdb.connect(config={"extension_directory": str(directory)}) as conn:
            for name in EXTENSIONS:
                # Loading checks the official extension signature and engine ABI.
                conn.execute(f"LOAD {name}")


if __name__ == "__main__":
    main()
