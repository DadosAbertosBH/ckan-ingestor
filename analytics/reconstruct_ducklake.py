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
"""Reconstruct the DuckLake catalog by running generated ``ducklake_add_data_files`` SQL.

Connects to the DuckLake catalog (Postgres) through the ``duckdb`` Python module and
executes the ``add_data_files_*.sql`` files. Credentials are read from environment
variables when present (in-cluster), falling back to the Kubernetes secrets (local
dev over Tailscale).
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import subprocess
import sys
from pathlib import Path

import duckdb
from tqdm import tqdm

CATALOG_HOST = "orchestrator-ingestor-orchestrator-postgres-tailscale.tail49b842.ts.net"
S3_ENDPOINT = "d8915a6c9b1e24b2ded7d9cd72318cdd.r2.cloudflarestorage.com"
S3_BUCKET = "public-datasets"

ENV_PASSWORD = "DUCKLAKE_PG_PASSWORD"
ENV_ACCESS_KEY = "DUCKLAKE_DATA_PATH__ACCESS_KEY_ID"
ENV_SECRET_KEY = "DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY"


def read_secret(name: str, key: str) -> str:
    raw = subprocess.run(
        [
            "kubectl",
            "get",
            "secret",
            name,
            "-n",
            "orchestrator",
            "-o",
            f"jsonpath={{.data.{key}}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return base64.b64decode(raw).decode("utf-8")


def resolve_credentials() -> tuple[str, str, str]:
    """Return ``(password, access_key, secret_key)`` from env vars or kubectl.

    Environment variables win (they are how the in-cluster Job injects the
    mounted Secrets); without them the local kubectl lookup is used.
    """
    password = os.environ.get(ENV_PASSWORD) or read_secret(
        "ingestor-orchestrator-postgres-app", "password"
    )
    access_key = os.environ.get(ENV_ACCESS_KEY) or read_secret(
        "ingestor-orchestrator-csi", "DUCKLAKE_DATA_PATH__ACCESS_KEY_ID"
    )
    secret_key = os.environ.get(ENV_SECRET_KEY) or read_secret(
        "ingestor-orchestrator-csi", "DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY"
    )
    return password, access_key, secret_key


def build_setup(
    password: str, access_key: str, secret_key: str, host: str = CATALOG_HOST
) -> str:
    """Return the DuckDB session setup SQL (R2 secret + catalog ATTACH).

    The R2 credentials are exposed through a shared ``TYPE S3`` secret rather
    than ``SET s3_*`` settings: the DuckLake extension reads the parquet footer
    for ``ducklake_add_data_files`` on an internal connection that shares the
    secret manager, but does not inherit per-connection settings.
    """
    return "\n".join(
        [
            "INSTALL postgres;",
            "INSTALL httpfs;",
            "INSTALL ducklake;",
            "LOAD postgres;",
            "LOAD httpfs;",
            "LOAD ducklake;",
            "CREATE SECRET r2 (TYPE S3, "
            f"KEY_ID '{access_key}', SECRET '{secret_key}', "
            f"REGION 'auto', ENDPOINT '{S3_ENDPOINT}', "
            "URL_STYLE 'path', USE_SSL true);",
            "SET enable_geoparquet_conversion = false;",
            "ATTACH IF NOT EXISTS "
            f"'ducklake:postgres:dbname=app host={host} port=5432 "
            f"user=app password={password}' "
            f"AS lake (DATA_PATH 's3://{S3_BUCKET}');",
            "USE lake;",
            "",
        ]
    )


_CALL_RE = re.compile(
    r"^CALL\s+ducklake_add_data_files\('lake',\s*"
    r"'(?P<table>(?:[^']|'')*)',\s*'"
    r"(?P<path>(?:[^']|'')*)'\);$"
)


def _unescape_sql(value: str) -> str:
    return value.replace("''", "'")


def _parse_call(call: str) -> tuple[str, str]:
    """Return the (table, path) arguments of a ``ducklake_add_data_files`` CALL."""
    match = _CALL_RE.fullmatch(call.strip())
    if not match:
        raise ValueError(f"unexpected CALL statement: {call!r}")
    return _unescape_sql(match.group("table")), _unescape_sql(match.group("path"))


def _quote_identifier(value: str) -> str:
    return value.replace('"', '""')


def _quote_string(value: str) -> str:
    return value.replace("'", "''")


def drop_table_statement(call: str) -> str:
    """Return the ``DROP TABLE IF EXISTS`` that resets ``call``'s table.

    Safe for files added with ``ducklake_add_data_files``: DuckLake marks them
    ``created_by_ducklake = false`` and only removes files it owns, so the R2
    parquet is left intact.
    """
    table, _ = _parse_call(call)
    return f'DROP TABLE IF EXISTS lake.main."{_quote_identifier(table)}";'


def create_table_statement(call: str) -> str:
    """Derive the prerequisite ``CREATE TABLE ... LIMIT 0`` for a ``CALL``.

    ``ducklake_add_data_files`` does not create the table, so the empty table
    (with the parquet schema) must exist first.
    """
    table, path = _parse_call(call)
    return (
        f'CREATE TABLE IF NOT EXISTS lake.main."{_quote_identifier(table)}" '
        f"AS SELECT * FROM read_parquet('{_quote_string(path)}') LIMIT 0;"
    )


def add_data_files_statement(call: str) -> str:
    """Return the ``CALL`` with permissive column matching.

    ``read_parquet`` renames/auto-names a few edge-case columns (empty names
    become ``C0``/``_16``/..., duplicates gain a ``_1`` suffix) while the raw
    parquet footer keeps the original names. DuckLake treats those as extra
    columns (file -> table) and missing columns (table -> file); enabling both
    flags registers the file against the common columns.
    """
    table, path = _parse_call(call)
    return (
        "CALL ducklake_add_data_files('lake', "
        f"'{_quote_string(table)}', '{_quote_string(path)}', "
        "allow_missing => true, ignore_extra_columns => true);"
    )


def expand_statement(call: str) -> list[str]:
    """Return the idempotent statement sequence for one ``CALL``.

    Drop (if present) -> create empty table -> add the data file. Re-running
    yields the same final state without duplicating files.
    """
    return [
        drop_table_statement(call),
        create_table_statement(call),
        add_data_files_statement(call),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql-dir", default="/tmp/ducklake_add_data_files")
    parser.add_argument(
        "--files",
        default=None,
        help="comma-separated filenames; default every add_data_files_*.sql",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="run only the first N statements (0 = all)",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("DUCKLAKE_CATALOG_HOST", CATALOG_HOST),
        help="catalog Postgres host (default: Tailscale, or $DUCKLAKE_CATALOG_HOST)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="stop at the first failed table instead of continuing",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    password, access_key, secret_key = resolve_credentials()
    setup = build_setup(password, access_key, secret_key, host=args.host)

    sql_dir = Path(args.sql_dir)
    if args.files:
        files = [sql_dir / name for name in args.files.split(",")]
    else:
        files = sorted(sql_dir.glob("add_data_files_*.sql"))

    calls: list[tuple[Path, str]] = []
    for path in files:
        lines = path.read_text().splitlines()
        if args.limit:
            lines = lines[: args.limit]
        calls.extend((path, line) for line in lines)

    connection = duckdb.connect(":memory:")
    failures: list[str] = []
    try:
        connection.execute(setup)
        with tqdm(total=len(calls), unit="table") as bar:
            for path, call in calls:
                try:
                    for statement in expand_statement(call):
                        connection.execute(statement)
                except duckdb.Error as error:
                    message = f"{path.name}: {call}\n  {error}"
                    failures.append(message)
                    bar.write(message)
                    if args.fail_fast:
                        return 1
                bar.update(1)
    finally:
        connection.close()

    if failures:
        failed_file = sql_dir / "failed.txt"
        failed_file.write_text("\n".join(failures) + "\n")
        print(f"failed {len(failures)}/{len(calls)} tables -> {failed_file}")
        return 1
    print(f"done ({len(calls)} tables)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
