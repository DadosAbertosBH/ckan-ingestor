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
"""Generate DuckLake ``ducklake_add_data_files`` statements from Cloudflare R2.

Lists every object in the ``public-datasets`` bucket (cached in SQLite), keeps
the latest ``ducklake_<version>.parquet`` for each resource folder, and emits:

* one ``CALL`` per table, batched into files of ``--batch-size`` tables (1000);
* a plain list of removable orphan files (older versions, legacy ``main/{uuid}/``
  copies and the old ``ducklake-{uuid4}.parquet`` convention) for later removal.

The listing runs in parallel (10 threads) with a tqdm progress bar and is cached
locally; use ``--refresh`` to force a re-list.

Credentials are read from the ``ingestor-orchestrator-csi`` Secret in the
``orchestrator`` namespace (the DuckLake data path), unless overridden via
``--access-key-id`` / ``--secret-access-key`` or the ``R2_ACCESS_KEY_ID`` /
``R2_SECRET_ACCESS_KEY`` environment variables.
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import boto3
from botocore.config import Config
from tqdm import tqdm

DEFAULT_BUCKET = "public-datasets"
DEFAULT_ENDPOINT_URL = "https://d8915a6c9b1e24b2ded7d9cd72318cdd.r2.cloudflarestorage.com"
DEFAULT_CACHE_FILE = "ducklake_listing.sqlite"
DEFAULT_ORPHANS_FILE = "orphans.txt"
SECRET_NAMESPACE = "orchestrator"
SECRET_NAME = "ingestor-orchestrator-csi"
ACCESS_KEY_NAME = "DUCKLAKE_DATA_PATH__ACCESS_KEY_ID"
SECRET_KEY_NAME = "DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY"


@dataclass(frozen=True)
class Object:
    key: str
    last_modified: str
    size: int


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _is_uuid(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            value,
        )
    )


def classify_parquet(keys: Sequence[str]) -> tuple[dict[str, str], list[str]]:
    """Split parquet keys into (latest, orphans).

    ``latest`` maps each resource folder to its newest top-level
    ``ducklake_{version}.parquet`` (what the catalog references in place).
    ``orphans`` lists every other removable parquet: older versions, the legacy
    ``main/{uuid}/ducklake-{uuid7}.parquet`` copies and the old
    ``{uuid}/ducklake-{uuid4}.parquet`` convention. Metadata tables under
    ``main/{non-uuid}/`` are ignored.
    """
    by_folder: dict[str, list[str]] = {}
    orphans: list[str] = []
    for key in keys:
        if key.startswith("main/"):
            parts = key.split("/")
            if (
                len(parts) >= 3
                and _is_uuid(parts[1])
                and parts[2].endswith(".parquet")
            ):
                orphans.append(key)
            continue
        folder, separator, filename = key.partition("/")
        if not folder or not separator or not filename.endswith(".parquet"):
            continue
        if filename.startswith("ducklake-"):
            orphans.append(key)
        elif filename.startswith("ducklake_"):
            by_folder.setdefault(folder, []).append(key)

    latest: dict[str, str] = {}
    for folder, folder_keys in by_folder.items():
        folder_keys.sort()
        latest[folder] = folder_keys[-1]
        orphans.extend(folder_keys[:-1])
    return latest, orphans


def render_statement(bucket: str, key: str) -> str:
    """Render one ``CALL ducklake_add_data_files`` statement for ``key``."""
    folder = key.partition("/")[0]
    return (
        "CALL ducklake_add_data_files('lake', "
        f"'{_escape_sql(folder)}', 's3://{bucket}/{_escape_sql(key)}');"
    )


def chunked(items: Sequence[str], size: int) -> Iterator[list[str]]:
    """Yield ``items`` in consecutive chunks of at most ``size`` elements."""
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


def read_secret_key(key: str) -> str:
    raw = subprocess.run(
        [
            "kubectl",
            "get",
            "secret",
            SECRET_NAME,
            "-n",
            SECRET_NAMESPACE,
            "-o",
            f"jsonpath={{.data.{key}}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return base64.b64decode(raw).decode("utf-8")


def resolve_credentials(args: argparse.Namespace) -> tuple[str, str]:
    access_key_id = args.access_key_id or os.environ.get("R2_ACCESS_KEY_ID")
    secret_access_key = args.secret_access_key or os.environ.get("R2_SECRET_ACCESS_KEY")
    if not access_key_id:
        access_key_id = read_secret_key(ACCESS_KEY_NAME)
    if not secret_access_key:
        secret_access_key = read_secret_key(SECRET_KEY_NAME)
    return access_key_id, secret_access_key


def _s3_client(endpoint_url: str, access_key_id: str, secret_access_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_objects(
    bucket: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
) -> list[Object]:
    """List every object in ``bucket`` with a single paginated scan.

    A single ``list_objects_v2`` scan issues one request per page (thousands of
    objects each), which is orders of magnitude faster than one request per
    folder when the bucket holds many small folders.
    """
    client = _s3_client(endpoint_url, access_key_id, secret_access_key)
    paginator = client.get_paginator("list_objects_v2")
    objects: list[Object] = []
    bar = tqdm(desc="Listing objects", unit="obj")
    try:
        for page in paginator.paginate(Bucket=bucket):
            for content in page.get("Contents", []):
                objects.append(
                    Object(
                        key=content["Key"],
                        last_modified=str(content["LastModified"]),
                        size=content["Size"],
                    )
                )
                bar.update(1)
    finally:
        bar.close()
    return objects


def load_cache(cache_file: Path) -> list[Object] | None:
    if not cache_file.exists():
        return None
    try:
        with sqlite3.connect(cache_file) as connection:
            rows = connection.execute(
                "SELECT key, last_modified, size FROM objects"
            ).fetchall()
    except sqlite3.Error:
        return None
    return [Object(key=row[0], last_modified=row[1], size=row[2]) for row in rows]


def save_cache(cache_file: Path, objects: Sequence[Object]) -> None:
    with sqlite3.connect(cache_file) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS objects ("
            "key TEXT PRIMARY KEY, last_modified TEXT NOT NULL, size INTEGER NOT NULL)"
        )
        connection.execute("DELETE FROM objects")
        connection.executemany(
            "INSERT OR REPLACE INTO objects (key, last_modified, size) VALUES (?, ?, ?)",
            [(obj.key, obj.last_modified, obj.size) for obj in objects],
        )


def load_or_list_objects(
    bucket: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    cache_file: Path,
    refresh: bool,
) -> list[Object]:
    if not refresh:
        cached = load_cache(cache_file)
        if cached:
            sys.stderr.write(f"Loaded {len(cached)} objects from cache\n")
            return cached
    objects = list_objects(bucket, endpoint_url, access_key_id, secret_access_key)
    save_cache(cache_file, objects)
    sys.stderr.write(f"Listed {len(objects)} objects\n")
    return objects


def write_batches(
    statements: Sequence[str], output_dir: Path, prefix: str, batch_size: int
) -> int:
    """Write ``statements`` into ``prefix_0001.sql`` ... with ``batch_size`` lines."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for index, batch in enumerate(chunked(statements, batch_size), start=1):
        (output_dir / f"{prefix}_{index:04d}.sql").write_text("\n".join(batch) + "\n")
        written += 1
    return written


def write_orphans(
    orphan_keys: Sequence[str], bucket: str, output_dir: Path, filename: str
) -> None:
    """Write the orphan keys as ``s3://<bucket>/<key>`` URIs, one per line."""
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / filename).write_text(
        "".join(f"s3://{bucket}/{key}\n" for key in orphan_keys)
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--endpoint-url", default=DEFAULT_ENDPOINT_URL)
    parser.add_argument("--output-dir", default="ducklake_add_data_files")
    parser.add_argument("--prefix", default="add_data_files")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--limit", type=int, default=0, help="only emit the first N tables (0 = all)"
    )
    parser.add_argument("--cache-file", default=DEFAULT_CACHE_FILE)
    parser.add_argument(
        "--refresh", action="store_true", help="ignore the cache and re-list R2"
    )
    parser.add_argument("--orphans-file", default=DEFAULT_ORPHANS_FILE)
    parser.add_argument("--access-key-id", default=None)
    parser.add_argument("--secret-access-key", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    access_key_id, secret_access_key = resolve_credentials(args)
    objects = load_or_list_objects(
        args.bucket,
        args.endpoint_url,
        access_key_id,
        secret_access_key,
        Path(args.cache_file),
        args.refresh,
    )
    keys = [obj.key for obj in objects]
    latest, orphans = classify_parquet(keys)
    statements = [render_statement(args.bucket, key) for key in sorted(latest.values())]
    if args.limit:
        statements = statements[: args.limit]
    written = write_batches(statements, Path(args.output_dir), args.prefix, args.batch_size)
    write_orphans(orphans, args.bucket, Path(args.output_dir), args.orphans_file)
    size_by_key = {obj.key: obj.size for obj in objects}
    orphan_size = sum(size_by_key.get(key, 0) for key in orphans)
    print(
        f"objects={len(objects)} tables={len(latest)} statements={len(statements)} "
        f"files={written} orphans={len(orphans)} orphan_bytes={orphan_size}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
