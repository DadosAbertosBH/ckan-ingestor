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
"""Publish a native DuckLake catalog; all source connections are read-only.

Maintenance must honor explorer/*/protected-files.json for every retained
publication. Publication preserves data files and retains catalogs for active sessions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.exceptions import ClientError


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def log_event(event: str, **details) -> None:
    """Write progress events in a format Kubernetes can stream directly."""
    logging.warning(json.dumps({"event": event, **details}, sort_keys=True))


def connect():
    config = {
        "temp_directory": os.environ.get("TMPDIR", "/tmp"),
        "threads": "1",
        "preserve_insertion_order": "false",
    }
    if directory := os.environ.get("DUCKDB_EXTENSION_DIRECTORY"):
        config["extension_directory"] = directory
    return duckdb.connect(config=config)


def enrich_tables(conn, tables):
    available = {t["name"] for t in tables if t["schema"] == "main"}

    def rows(name):
        if name not in available:
            return {}
        result = conn.execute(f"SELECT * FROM lake.main.{identifier(name)}")
        columns = [d[0] for d in result.description]
        return (
            {
                str(row[columns.index("id")]): dict(zip(columns, row, strict=True))
                for row in result.fetchall()
            }
            if "id" in columns
            else {}
        )

    resources = rows("ckan_resource")
    datasets = rows("ckan_dataset")
    for table in tables:
        resource = resources.get(table["name"], {})
        dataset = datasets.get(
            str(resource.get("package_id") or resource.get("dataset_id")), {}
        )
        table["title"] = resource.get("name") or dataset.get("title") or table["name"]
        table["description"] = resource.get("description") or dataset.get("notes") or ""
        table["dataset"] = dataset.get("title") or ""
        table["source"] = (
            urlparse(dataset.get("url") or resource.get("url") or "").hostname or ""
        )


def configure_storage(conn):
    endpoint = os.environ.get("DUCKLAKE_DATA_PATH__ENDPOINT", "")
    if endpoint:
        conn.execute("LOAD httpfs")
        conn.execute(
            "CREATE SECRET explorer_source (TYPE S3, REGION 'auto', URL_STYLE 'path', "
            f"ENDPOINT {literal(endpoint.removeprefix('https://').rstrip('/'))}, "
            f"KEY_ID {literal(os.environ['DUCKLAKE_DATA_PATH__ACCESS_KEY_ID'])}, "
            f"SECRET {literal(os.environ['DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY'])})"
        )


def export_catalog(source: str, destination: Path, schema: str | None = None) -> dict:
    """Copy catalog tables in one transaction, then expire history on the copy.

    pg_use_ctid_scan=false keeps scans on the transaction's PostgreSQL
    connection, rather than distributing scans across independent snapshots.
    Metadata is copied as data tables to avoid PostgreSQL index incompatibility.
    """
    destination = Path(destination)
    postgres = source.startswith(("postgres:", "postgresql://", "postgres://"))
    schema = schema or ("public" if postgres else "main")
    if destination.exists():
        raise ValueError("destination must be a new file")
    started = time.monotonic()
    log_event("catalog_export_started", schema=schema, sourceType="postgres" if postgres else "duckdb")
    with tempfile.TemporaryDirectory(prefix="lake-export-") as work:
        copy_path = Path(work) / "copy.duckdb"
        with connect() as conn:
            if postgres:
                conn.execute("LOAD postgres")
                conn.execute("SET pg_use_ctid_scan=false")
            attach_source = source.removeprefix("postgres:") if postgres else source
            kind = ", TYPE POSTGRES" if postgres else ""
            conn.execute(f"ATTACH {literal(attach_source)} AS source (READ_ONLY{kind})")
            conn.execute(f"ATTACH {literal(str(copy_path))} AS exported")
            conn.execute("BEGIN TRANSACTION")
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='source' AND table_schema=? "
                "AND table_type='BASE TABLE' ORDER BY table_name",
                [schema],
            ).fetchall()
            names = [name for (name,) in tables if name.startswith("ducklake_")]
            if "ducklake_metadata" not in names:
                raise ValueError("source schema contains no DuckLake catalog")
            log_event("catalog_tables_discovered", tables=len(names))
            for position, name in enumerate(names, start=1):
                table_started = time.monotonic()
                log_event(
                    "catalog_table_copy_started",
                    table=name,
                    position=position,
                    total=len(names),
                )
                conn.execute(
                    f"CREATE TABLE exported.main.{identifier(name)} AS "
                    f"SELECT * FROM source.{identifier(schema)}.{identifier(name)}"
                )
                log_event(
                    "catalog_table_copy_completed",
                    table=name,
                    position=position,
                    total=len(names),
                    durationSeconds=round(time.monotonic() - table_started, 3),
                )
            conn.execute("COMMIT")
            log_event("catalog_copy_completed", tables=len(names))
        with connect() as conn:
            conn.execute("LOAD ducklake")
            configure_storage(conn)
            conn.execute(f"ATTACH {literal('ducklake:' + str(copy_path))} AS lake")
            snapshot = conn.sql(
                "SELECT max(snapshot_id) FROM lake.snapshots()"
            ).fetchone()[0]
            old = [
                row[0]
                for row in conn.sql(
                    "SELECT snapshot_id FROM lake.snapshots()"
                ).fetchall()
                if row[0] != snapshot
            ]
            if old:
                conn.execute(
                    "CALL ducklake_expire_snapshots('lake', versions => ?)", [old]
                )
            tables = []
            protected = set()
            files_by_table = {}
            lake_tables = conn.sql(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_catalog='lake' ORDER BY table_schema, table_name"
            ).fetchall()
            log_event(
                "catalog_snapshot_loaded",
                snapshotId=str(snapshot),
                expiredSnapshots=len(old),
                tables=len(lake_tables),
            )
            for position, (table_schema, name) in enumerate(lake_tables, start=1):
                table_started = time.monotonic()
                log_event(
                    "catalog_table_verification_started",
                    schema=table_schema,
                    table=name,
                    position=position,
                    total=len(lake_tables),
                )
                qualified = f"lake.{identifier(table_schema)}.{identifier(name)}"
                columns = [
                    {"name": row[0], "type": row[1]}
                    for row in conn.sql(f"DESCRIBE {qualified}").fetchall()
                ]
                # Full count also verifies access to inline data and delete files.
                count = conn.sql(f"SELECT count(*) FROM {qualified}").fetchone()[0]
                tables.append(
                    {
                        "schema": table_schema,
                        "name": name,
                        "title": name,
                        "columns": columns,
                        "rows": str(count),
                    }
                )
                files = conn.execute(
                    "SELECT * FROM ducklake_list_files('lake', ?, schema => ?)",
                    [name, table_schema],
                )
                field_names = [d[0] for d in files.description]
                table_files = set()
                for row in files.fetchall():
                    for key, value in zip(field_names, row, strict=True):
                        if key in ("data_file", "delete_file") and value:
                            protected.add(value)
                            table_files.add(value)
                files_by_table[f"{table_schema}.{name}"] = sorted(table_files)
                log_event(
                    "catalog_table_verified",
                    schema=table_schema,
                    table=name,
                    position=position,
                    total=len(lake_tables),
                    rows=str(count),
                    files=len(table_files),
                    durationSeconds=round(time.monotonic() - table_started, 3),
                )
            enrich_tables(conn, tables)
            format_version = conn.sql(
                "SELECT value FROM __ducklake_metadata_lake.ducklake_metadata WHERE key='version'"
            ).fetchone()[0]
            extension = conn.sql(
                "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'"
            ).fetchone()[0]
        # Rewriting into a new database releases pages occupied by expired history.
        with connect() as conn:
            log_event("catalog_compaction_started")
            conn.execute(f"ATTACH {literal(str(copy_path))} AS compacted (READ_ONLY)")
            conn.execute(f"ATTACH {literal(str(destination))} AS final")
            conn.execute("COPY FROM DATABASE compacted TO final")
            log_event("catalog_compaction_completed")
    with destination.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    now = datetime.now(UTC)
    manifest = {
        "id": now.strftime("%Y%m%dT%H%M%S") + f"-{snapshot}-{digest[:12]}",
        "createdAt": now.isoformat(),
        "snapshotId": str(snapshot),
        "duckdbVersion": duckdb.__version__,
        "ducklakeVersion": extension,
        "formatVersion": format_version,
        "sha256": digest,
        "size": destination.stat().st_size,
        "tables": tables,
        "protectedFiles": sorted(protected),
        "filesByTable": files_by_table,
    }
    log_event(
        "catalog_export_completed",
        snapshotId=str(snapshot),
        size=manifest["size"],
        durationSeconds=round(time.monotonic() - started, 3),
    )
    return manifest


def publish_snapshot(client, bucket: str, catalog: Path, manifest: dict) -> dict:
    prefix = f"explorer/{manifest['id']}"
    key = f"{prefix}/catalog.duckdb"
    log_event("catalog_upload_started", id=manifest["id"], size=manifest["size"])
    client.upload_file(
        str(catalog),
        bucket,
        key,
        ExtraArgs={
            "Metadata": {"sha256": manifest["sha256"]},
            "ContentType": "application/octet-stream",
        },
    )
    remote = client.head_object(Bucket=bucket, Key=key)
    if (
        remote["ContentLength"] != manifest["size"]
        or remote["Metadata"].get("sha256") != manifest["sha256"]
    ):
        raise ValueError("uploaded catalog verification failed")
    log_event("catalog_upload_verified", id=manifest["id"])
    published = {
        k: v for k, v in manifest.items() if k not in ("protectedFiles", "filesByTable")
    }
    published["catalogKey"] = key
    published["protectedFilesKey"] = f"{prefix}/protected-files.json"
    published["bucket"] = bucket
    previous = read_object_json(client, bucket, "explorer/latest.json")
    if previous:
        published["previousId"] = (
            previous.get("previousId")
            if previous.get("id") == manifest["id"]
            else previous["id"]
        )

    def put(key, value):
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(value).encode(),
            ContentType="application/json",
            CacheControl="no-store",
        )

    protection = (
        {"files": manifest["protectedFiles"], "tables": manifest["filesByTable"]}
        if "filesByTable" in manifest
        else manifest.get("protectedFiles", [])
    )
    put(published["protectedFilesKey"], protection)
    put(f"{prefix}/manifest.json", published)
    put("explorer/latest.json", published)
    log_event("catalog_published", id=manifest["id"])
    return published


def retained_publications(publications, current_id, previous_id, now):
    cutoff = now - timedelta(hours=48, minutes=15)
    return {
        p["id"]
        for p in publications
        if p["id"] in (current_id, previous_id)
        or datetime.fromisoformat(p["createdAt"]) >= cutoff
    }


def read_object_json(client, bucket, key):
    try:
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def list_publications(client, bucket):
    publications = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="explorer/", Delimiter="/"
    ):
        for item in page.get("CommonPrefixes", []):
            prefix = item["Prefix"]
            if re.fullmatch(r"explorer/[A-Za-z0-9_-]+/", prefix):
                manifest = read_object_json(client, bucket, prefix + "manifest.json")
                if manifest:
                    publications.append(manifest)
    return publications


def prune_publications(client, bucket, current):
    """Remove only explorer artifacts, with a grace period for issued sessions."""
    publications = list_publications(client, bucket)
    retained = retained_publications(
        publications, current["id"], current.get("previousId"), datetime.now(UTC)
    )
    for publication in publications:
        name = publication["id"]
        if name not in retained and re.fullmatch(r"[A-Za-z0-9_-]+", name):
            # Keep the manifest visible until its catalog has been removed.
            for filename in ("catalog.duckdb", "manifest.json", "protected-files.json"):
                client.delete_object(Bucket=bucket, Key=f"explorer/{name}/{filename}")


def protected_files(client, bucket):
    """Return the conservative exclusion set for every published catalog.

    Data-file maintenance must subtract this set from candidates, and must
    coordinate with the exporter before deleting files or rewriting snapshots.
    """
    files = set()
    for publication in list_publications(client, bucket):
        index = read_object_json(client, bucket, publication["protectedFilesKey"])
        if index is None:
            raise ValueError("publication protection index is missing")
        files.update(index if isinstance(index, list) else index["files"])
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=os.environ.get("DUCKLAKE_CATALOG_URI"))
    parser.add_argument("--schema", default=os.environ.get("DUCKLAKE_METADATA_SCHEMA"))
    parser.add_argument(
        "--protected-files",
        action="store_true",
        help="Print the exclusion set for data-file maintenance",
    )
    args = parser.parse_args()
    if not args.source and not args.protected_files:
        parser.error("--source or DUCKLAKE_CATALOG_URI is required")
    started = time.monotonic()
    bucket = os.environ.get("DUCKLAKE_DATA_PATH__BUCKET", "public-datasets")
    endpoint = os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"]
    client = boto3.client(
        "s3",
        endpoint_url="https://" + endpoint.removeprefix("https://").rstrip("/"),
        aws_access_key_id=os.environ["DUCKLAKE_DATA_PATH__ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    if args.protected_files:
        print(json.dumps(sorted(protected_files(client, bucket))))
        return
    with tempfile.TemporaryDirectory(prefix="lake-snapshot-") as work:
        path = Path(work) / "catalog.duckdb"
        manifest = export_catalog(args.source, path, schema=args.schema)
        published = publish_snapshot(client, bucket, path, manifest)
        prune_publications(client, bucket, published)
        log_event(
            "snapshot_published",
            id=manifest["id"],
            size=manifest["size"],
            durationSeconds=round(time.monotonic() - started, 3),
        )


if __name__ == "__main__":
    main()
