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
"""Import source JSON snapshots through source-scoped collection replacement."""

import json
import re
from datetime import datetime
from time import monotonic


RESOURCE_COLUMNS = {
    "source_id": "VARCHAR",
    "id": "VARCHAR",
    "parent_resource_id": "VARCHAR",
    "kind": "VARCHAR",
    "title": "VARCHAR",
    "description": "VARCHAR",
    "page_url": "VARCHAR",
    "content_url": "VARCHAR",
    "format": "VARCHAR",
    "source_version": "VARCHAR",
    "dataset_name": "VARCHAR",
    "metadata": "JSON",
    "metadata_collection": "VARCHAR",
    "status": "VARCHAR",
    "deleted": "BOOLEAN",
}
SOURCE_COLUMNS = {
    "source_id": "VARCHAR",
    "id": "VARCHAR",
    "title": "VARCHAR",
    "type": "VARCHAR",
    "url": "VARCHAR",
}
RAW_COLUMNS = {
    "source_id": "VARCHAR",
    "id": "VARCHAR",
    "parent_resource_id": "VARCHAR",
    "metadata": "JSON",
    "sync_id": "VARCHAR",
    "collected_at": "TIMESTAMPTZ",
}


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _rows(conn, name, source_id):
    result = conn.execute(
        f"SELECT * FROM lake.main.{identifier(name)} WHERE source_id=?", [source_id]
    )
    columns = [column[0] for column in result.description]
    rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    for row in rows:
        for key in ("metadata",):
            if isinstance(row.get(key), str):
                row[key] = json.loads(row[key])
    return rows


def _ensure_collection(conn, name, columns):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ValueError("invalid collection name")
    if columns.get("source_id") != "VARCHAR":
        raise ValueError("collections require source_id")
    if any(
        kind not in {"VARCHAR", "JSON", "BOOLEAN", "BIGINT", "TIMESTAMPTZ"}
        for kind in columns.values()
    ):
        raise ValueError("invalid column type")
    definitions = ",".join(
        f"{identifier(column)} {kind}" for column, kind in columns.items()
    )
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS lake.main.{identifier(name)} ({definitions})"
    )


def ingest_snapshot(conn, snapshot, emit=None):
    source = snapshot["source"]
    source_id, sync_id = source["id"], snapshot["sync_id"]
    emit = emit or (lambda _event, **_details: None)
    at = datetime.fromisoformat(snapshot["collected_at"].replace("Z", "+00:00"))
    if not source_id or not sync_id or at.tzinfo is None:
        raise ValueError("source, sync identity and timezone are required")
    records = snapshot["resources"]
    parents = {}
    for record in records:
        key = record.get("id")
        if not key or key in parents or record.get("source_id", source_id) != source_id:
            raise ValueError("invalid, duplicate or foreign resource identity")
        parents[key] = record.get("parent_resource_id")
    visited = set()
    for key in parents:
        path = set()
        while key and key not in visited:
            if key not in parents or key in path:
                raise ValueError("resource tree contains a missing parent or cycle")
            path.add(key)
            key = parents[key]
        visited.update(path)

    started = monotonic()
    emit(
        "catalog_snapshot_ingest_started",
        sourceId=source_id,
        syncId=sync_id,
        resourcesTotal=len(records),
    )
    conn.execute("BEGIN")
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lake.catalog_imports(source_id VARCHAR, sync_id VARCHAR, collected_at TIMESTAMPTZ)"
        )
        newer = conn.execute(
            "SELECT count(*) FROM lake.catalog_imports WHERE source_id=? AND collected_at>=?",
            [source_id, at],
        ).fetchone()[0]
        replay = conn.execute(
            "SELECT count(*) FROM lake.catalog_imports WHERE source_id=? AND sync_id=?",
            [source_id, sync_id],
        ).fetchone()[0]
        if replay or newer:
            conn.execute("ROLLBACK")
            return False
        preparation_started = monotonic()
        current = []
        _ensure_collection(conn, "catalog_resources", RESOURCE_COLUMNS)
        previous = {
            row["id"]: row for row in _rows(conn, "catalog_resources", source_id)
        }
        for position, resource in enumerate(records, start=1):
            row = {key: resource.get(key) for key in RESOURCE_COLUMNS}
            row.update(source_id=source_id, deleted=False)
            old = previous.get(row["id"], {})
            row["status"] = (
                old.get("status")
                if old.get("source_version") == row.get("source_version")
                and not old.get("deleted")
                else "discovered"
            )
            row["status"] = row["status"] or "discovered"
            row["title"] = row["title"] or _resource_title(row)
            current.append(row)
            if position % 100 == 0:
                emit(
                    "catalog_inbox_snapshot_import_progress",
                    sourceId=source_id,
                    syncId=sync_id,
                    resourcesRead=position,
                )
        for key, old in previous.items():
            if key not in parents:
                current.append(dict(old, deleted=True, status="deleted"))
        emit(
            "catalog_snapshot_resources_prepared",
            sourceId=source_id,
            syncId=sync_id,
            resourcesPrepared=len(current),
            durationSeconds=round(monotonic() - preparation_started, 3),
        )
        _replace_collection_with_progress(
            conn,
            "catalog_resources",
            source_id,
            sync_id,
            current,
            RESOURCE_COLUMNS,
            emit,
        )
        _replace_collection_with_progress(
            conn,
            "catalog_sources",
            source_id,
            sync_id,
            [dict(source, source_id=source_id)],
            SOURCE_COLUMNS,
            emit,
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lake.catalog_collections(source_id VARCHAR, name VARCHAR)"
        )
        declared = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM lake.catalog_collections WHERE source_id=?",
                [source_id],
            ).fetchall()
        }
        declared.update(
            resource["metadata_collection"]
            for resource in records
            if resource.get("metadata_collection")
        )
        for name in sorted(declared):
            if name.startswith("catalog_"):
                raise ValueError("metadata collection name is reserved")
            raw = [
                dict(
                    {key: resource.get(key) for key in RAW_COLUMNS},
                    sync_id=sync_id,
                    collected_at=at,
                )
                for resource in records
                if resource.get("metadata_collection") == name
            ]
            _replace_collection_with_progress(
                conn, name, source_id, sync_id, raw, RAW_COLUMNS, emit
            )
        _replace_collection_with_progress(
            conn,
            "catalog_collections",
            source_id,
            sync_id,
            [{"name": name} for name in sorted(declared)],
            {"source_id": "VARCHAR", "name": "VARCHAR"},
            emit,
        )
        conn.execute(
            "INSERT INTO lake.catalog_imports VALUES (?,?,?)", [source_id, sync_id, at]
        )
        conn.execute("COMMIT")
        emit(
            "catalog_snapshot_ingest_completed",
            sourceId=source_id,
            syncId=sync_id,
            durationSeconds=round(monotonic() - started, 3),
        )
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _resource_title(row):
    from urllib.parse import unquote, urlparse

    metadata = row.get("metadata") or {}
    return (
        metadata.get("name")
        or unquote(urlparse(row.get("content_url") or "").path.rsplit("/", 1)[-1])
        or "Recurso sem título"
    )


def replace_collection(conn, name, source_id, rows, columns):
    """Replace one source's records; the caller owns the enclosing transaction."""
    _ensure_collection(conn, name, columns)
    values = []
    for row in rows:
        if row.get("source_id") not in (None, source_id):
            raise ValueError("foreign source in collection")
        row = dict(row, source_id=source_id)
        values.append(
            [
                _json(row.get(key)) if kind == "JSON" else row.get(key)
                for key, kind in columns.items()
            ]
        )
    conn.execute(
        f"DELETE FROM lake.main.{identifier(name)} WHERE source_id=?", [source_id]
    )
    if values:
        placeholders = ",".join("?" for _ in columns)
        names = ",".join(identifier(key) for key in columns)
        conn.executemany(
            f"INSERT INTO lake.main.{identifier(name)} ({names}) VALUES ({placeholders})",
            values,
        )


def _replace_collection_with_progress(
    conn, name, source_id, sync_id, rows, columns, emit
):
    started = monotonic()
    emit(
        "catalog_collection_replace_started",
        sourceId=source_id,
        syncId=sync_id,
        collection=name,
        rows=len(rows),
    )
    replace_collection(conn, name, source_id, rows, columns)
    emit(
        "catalog_collection_replace_completed",
        sourceId=source_id,
        syncId=sync_id,
        collection=name,
        rows=len(rows),
        durationSeconds=round(monotonic() - started, 3),
    )


def physical_resource_id(resource_id):
    return resource_id


def refresh_table_links(conn, metadata_schema="main"):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS lake.catalog_tables(source_id VARCHAR, resource_id VARCHAR, table_schema VARCHAR, table_name VARCHAR)"
    )
    names = {
        row[0]
        for row in conn.sql(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog='lake' AND table_schema='main'"
        ).fetchall()
    }
    if "catalog_resources" not in names:
        return
    versions = {
        (source, resource): version or ""
        for source, resource, version in conn.sql(
            "SELECT source_id,id,source_version FROM lake.catalog_resources"
        ).fetchall()
    }
    resources = list(versions)
    existing = {
        (schema, name)
        for schema, name in conn.sql(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_catalog='lake'"
        ).fetchall()
    }
    tags = {}
    metadata = "__ducklake_metadata_lake." + identifier(metadata_schema)
    for name, schema, key, value in conn.sql(
        f"SELECT t.table_name,s.schema_name,g.key,g.value FROM {metadata}.ducklake_tag g JOIN {metadata}.ducklake_table t ON t.table_id=g.object_id JOIN {metadata}.ducklake_schema s ON s.schema_id=t.schema_id WHERE g.end_snapshot IS NULL AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL AND g.key IN ('source_id','resource_id','source_version')"
    ).fetchall():
        tags.setdefault((schema, name), {})[key] = value
    resource_keys = set(resources)
    links = {}
    # An explicit tag is authoritative, including tables produced from subresources.
    for (schema, name), values in tags.items():
        key = (values.get("source_id"), values.get("resource_id"))
        if key in resource_keys and (schema, name) in existing:
            links[(schema, name)] = (key[0], key[1], schema, name)
    for source, resource in resources:
        candidate = (
            physical_resource_id(resource)
            if ("main", physical_resource_id(resource)) in existing
            else None
        )
        if candidate and ("main", candidate) not in links:
            links[("main", candidate)] = (source, resource, "main", candidate)
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM lake.catalog_tables")
        if links:
            conn.executemany(
                "INSERT INTO lake.catalog_tables VALUES (?,?,?,?)", list(links.values())
            )
            completed = [
                (source, resource)
                for source, resource, schema, name in links.values()
                if tags.get((schema, name), {}).get("source_version", "")
                == versions[(source, resource)]
            ]
            if completed:
                conn.executemany(
                    "UPDATE lake.catalog_resources SET status='completed' WHERE source_id=? AND id=? AND status='discovered' AND NOT deleted",
                    completed,
                )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def import_inbox(conn, client, bucket, metadata_schema="main", emit=None):
    """S3 PUT is atomic: only complete JSONL discoveries become visible here."""
    from urllib.parse import unquote

    emit = emit or (lambda _event, **_details: None)
    emit("catalog_inbox_scan_started", bucket=bucket)
    has_imports = conn.sql(
        "SELECT count(*) FROM information_schema.tables WHERE table_catalog='lake' AND table_schema='main' AND table_name='catalog_imports'"
    ).fetchone()[0]
    imported = (
        set(conn.sql("SELECT source_id,sync_id FROM lake.catalog_imports").fetchall())
        if has_imports
        else set()
    )
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="catalog-inbox/"
    ):
        keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item["Key"].endswith(".jsonl")
        )
    emit(
        "catalog_inbox_scan_completed",
        artifacts=len(keys),
        importedSnapshots=len(imported),
    )
    discoveries = []
    for key in keys:
        parts = key.split("/")
        if len(parts) != 3:
            raise ValueError("invalid catalog artifact path")
        identity = (unquote(parts[1]), unquote(parts[2].removesuffix(".jsonl")))
        discovery_started = monotonic()
        emit(
            "catalog_inbox_snapshot_discovery_started",
            sourceId=identity[0],
            syncId=identity[1],
        )
        response = client.get_object(Bucket=bucket, Key=key)
        try:
            lines = iter(response["Body"].iter_lines())
            header = json.loads(next(lines))
            if header.get("version") != 1:
                raise ValueError("unsupported source snapshot version")
            if (header["source"]["id"], header["sync_id"]) != identity:
                raise ValueError("catalog artifact identity mismatch")
            resources_total = header.get("resources_total")
            if resources_total is not None and (
                isinstance(resources_total, bool)
                or not isinstance(resources_total, int)
                or resources_total < 0
            ):
                raise ValueError("catalog resource total must be a non-negative integer")
            discoveries.append(
                (key, header, resources_total, monotonic() - discovery_started)
            )
        finally:
            response["Body"].close()

    latest = {}
    for discovery in discoveries:
        key, header, _, _ = discovery
        source_id = header["source"]["id"]
        collected_at = datetime.fromisoformat(
            header["collected_at"].replace("Z", "+00:00")
        )
        previous = latest.get(source_id)
        if previous is None or (collected_at, key) > (
            datetime.fromisoformat(previous[1]["collected_at"].replace("Z", "+00:00")),
            previous[0],
        ):
            latest[source_id] = discovery
    snapshots = []
    for key, header, resources_total, duration in discoveries:
        source_id, sync_id = header["source"]["id"], header["sync_id"]
        selected = latest[source_id]
        if key != selected[0]:
            emit(
                "catalog_inbox_snapshot_superseded",
                sourceId=source_id,
                syncId=sync_id,
                latestSyncId=selected[1]["sync_id"],
            )
            continue
        if (source_id, sync_id) in imported:
            emit(
                "catalog_inbox_snapshot_skipped",
                sourceId=source_id,
                syncId=sync_id,
            )
            continue
        details = {
            "sourceId": source_id,
            "syncId": sync_id,
            "durationSeconds": round(duration, 3),
        }
        if resources_total is not None:
            details["resourcesTotal"] = resources_total
        emit("catalog_inbox_snapshot_discovered", **details)
        snapshots.append((key, header))
    # Read one pending discovery at a time, rather than retaining its full history in memory.
    for key, header in sorted(
        snapshots,
        key=lambda value: datetime.fromisoformat(
            value[1]["collected_at"].replace("Z", "+00:00")
        ),
    ):
        started = monotonic()
        source_id, sync_id = header["source"]["id"], header["sync_id"]
        emit(
            "catalog_inbox_snapshot_import_started",
            sourceId=source_id,
            syncId=sync_id,
        )
        read_started = monotonic()
        emit(
            "catalog_inbox_snapshot_read_started",
            sourceId=source_id,
            syncId=sync_id,
        )
        response = client.get_object(Bucket=bucket, Key=key)
        try:
            lines = iter(response["Body"].iter_lines())
            next(lines)
            resources = []
            for line in lines:
                if not line.strip():
                    continue
                resources.append(json.loads(line))
            emit(
                "catalog_inbox_snapshot_read_completed",
                sourceId=source_id,
                syncId=sync_id,
                resourcesRead=len(resources),
                durationSeconds=round(monotonic() - read_started, 3),
            )
            changed = ingest_snapshot(
                conn,
                dict(
                    header,
                    resources=resources,
                ),
                emit=emit,
            )
            emit(
                "catalog_inbox_snapshot_import_completed",
                sourceId=source_id,
                syncId=sync_id,
                imported=changed,
                durationSeconds=round(monotonic() - started, 3),
            )
        finally:
            response["Body"].close()
    conn.execute("DROP TABLE IF EXISTS lake.catalog_results")
    conn.execute("DROP TABLE IF EXISTS lake.catalog_artifacts")
    emit("catalog_table_links_refresh_started")
    refresh_table_links(conn, metadata_schema=metadata_schema)
    emit("catalog_table_links_refresh_completed")
