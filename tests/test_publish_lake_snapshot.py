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
import importlib.util
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import duckdb
import pytest
from botocore.exceptions import ClientError

spec = importlib.util.spec_from_file_location(
    "publish_lake_snapshot",
    Path(__file__).resolve().parents[1] / "analytics/publish_lake_snapshot.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
export_catalog = module.export_catalog
publish_snapshot = module.publish_snapshot


def test_connect_uses_writable_temp_directory(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setattr(
        module.duckdb,
        "connect",
        lambda **kwargs: captured.update(kwargs) or Mock(),
    )

    module.connect()

    assert captured["config"]["temp_directory"] == str(tmp_path)


def storage_client():
    client = Mock()
    client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "GetObject"
    )
    return client


@pytest.fixture
def lake(tmp_path):
    catalog = tmp_path / "source.duckdb"
    conn = duckdb.connect()
    conn.execute("LOAD ducklake")
    conn.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{tmp_path}/data')")
    conn.execute("CREATE TABLE lake.people(id INTEGER, label VARCHAR)")
    conn.execute("INSERT INTO lake.people VALUES (1, 'old'), (2, 'kept')")
    conn.execute("DELETE FROM lake.people WHERE id=1")
    conn.execute("ALTER TABLE lake.people RENAME label TO name")
    conn.execute("CREATE TABLE lake.empty_table(value BIGINT)")
    conn.execute("CREATE TABLE lake.external AS SELECT range AS id FROM range(5000)")
    conn.execute("DELETE FROM lake.external WHERE id % 2 = 0")
    conn.execute("CREATE TABLE unrelated(secret VARCHAR)")
    expected = conn.sql("SELECT * FROM lake.people").fetchall()
    snapshot = conn.sql("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    conn.close()
    return catalog, expected, snapshot


def test_native_catalog_keeps_latest_state_and_inline_rows(lake, tmp_path):
    source, expected, snapshot = lake
    result = export_catalog(str(source), tmp_path / "published.duckdb")
    assert result["snapshotId"] == str(snapshot)
    assert result["size"] == (tmp_path / "published.duckdb").stat().st_size
    assert len(result["sha256"]) == 64
    conn = duckdb.connect()
    conn.execute("LOAD ducklake")
    conn.execute(f"ATTACH 'ducklake:{tmp_path}/published.duckdb' AS copy (READ_ONLY)")
    assert conn.sql("SELECT * FROM copy.people").fetchall() == expected
    assert conn.sql("SELECT count(*) FROM copy.external").fetchone()[0] == 2500
    assert conn.sql("SELECT count(*) FROM copy.empty_table").fetchone()[0] == 0
    assert conn.sql("SELECT count(*) FROM copy.snapshots()").fetchone()[0] == 1
    assert {t["name"] for t in result["tables"]} == {
        "people",
        "empty_table",
        "external",
    }
    assert all(Path(p).exists() for p in result["protectedFiles"])
    assert result["protectedFiles"]
    assert result["filesByTable"]["main.external"]
    conn.close()
    with duckdb.connect(str(source), read_only=True) as conn:
        assert conn.sql("SELECT count(*) FROM ducklake_snapshot").fetchone()[0] > 1


def test_publication_only_advances_latest_after_verified_upload(tmp_path):
    path = tmp_path / "catalog.duckdb"
    path.write_bytes(b"catalog")
    client = storage_client()
    client.head_object.return_value = {
        "ContentLength": 7,
        "Metadata": {"sha256": "abc"},
    }
    manifest = {
        "id": "v1",
        "sha256": "abc",
        "size": 7,
        "protectedFiles": ["s3://lake/data.parquet"],
    }
    publish_snapshot(client, "lake", path, manifest)
    keys = [call.kwargs["Key"] for call in client.put_object.call_args_list]
    assert keys[-1] == "explorer/latest.json"
    assert keys.index("explorer/v1/protected-files.json") < len(keys) - 1


def test_failed_upload_preserves_current_publication(tmp_path):
    path = tmp_path / "catalog.duckdb"
    path.write_bytes(b"catalog")
    client = storage_client()
    client.upload_file.side_effect = RuntimeError("interrupted")
    with pytest.raises(RuntimeError, match="interrupted"):
        publish_snapshot(client, "lake", path, {"id": "v1", "sha256": "abc", "size": 7})
    assert not client.put_object.called


def test_resource_titles_and_sources_come_from_ckan_metadata(lake, tmp_path):
    source, _, _ = lake
    with duckdb.connect() as conn:
        conn.execute("LOAD ducklake")
        conn.execute(f"ATTACH 'ducklake:{source}' AS lake")
        conn.execute(
            "CREATE TABLE lake.ckan_resource(id VARCHAR, name VARCHAR, package_id VARCHAR, description VARCHAR)"
        )
        conn.execute(
            "INSERT INTO lake.ckan_resource VALUES ('people', 'Pessoas cadastradas', 'dataset-1', 'Descrição legível')"
        )
        conn.execute(
            "CREATE TABLE lake.ckan_dataset(id VARCHAR, title VARCHAR, url VARCHAR)"
        )
        conn.execute(
            "INSERT INTO lake.ckan_dataset VALUES ('dataset-1', 'Cadastro municipal', 'https://dados.example/dataset/cadastro')"
        )
    result = export_catalog(str(source), tmp_path / "published.duckdb")
    table = next(t for t in result["tables"] if t["name"] == "people")
    assert table["title"] == "Pessoas cadastradas"
    assert table["source"] == "dados.example"
    assert table["dataset"] == "Cadastro municipal"
    assert table["description"] == "Descrição legível"


def test_retention_protects_current_previous_and_recent_sessions():
    publications = [
        {"id": name, "createdAt": time}
        for name, time in [
            ("current", "2020-01-01T00:00:00+00:00"),
            ("previous", "2019-01-01T00:00:00+00:00"),
            ("recent", "2026-10-20T00:00:00+00:00"),
            ("expiring-session", "2026-10-19T11:50:00+00:00"),
            ("old", "2026-10-18T00:00:00+00:00"),
        ]
    ]
    retained = module.retained_publications(
        publications, "current", "previous", datetime(2026, 10, 21, 12, tzinfo=UTC)
    )
    assert retained == {"current", "previous", "recent", "expiring-session"}


def test_publication_records_previous_and_protection_for_maintenance(tmp_path):
    path = tmp_path / "catalog.duckdb"
    path.write_bytes(b"catalog")
    client = storage_client()
    client.get_object.side_effect = None
    client.head_object.return_value = {
        "ContentLength": 7,
        "Metadata": {"sha256": "abc"},
    }
    client.get_object.return_value = {"Body": Mock(read=lambda: b'{"id":"previous"}')}
    result = publish_snapshot(
        client,
        "lake",
        path,
        {"id": "v1", "sha256": "abc", "size": 7, "protectedFiles": []},
    )
    assert result["previousId"] == "previous"


@pytest.mark.skipif(
    not os.environ.get("LAKE_TEST_POSTGRES_DSN"),
    reason="isolated PostgreSQL DSN required",
)
def test_postgres_catalog_export_ignores_indexes_and_preserves_state(lake, tmp_path):
    source, expected, _ = lake
    dsn = os.environ["LAKE_TEST_POSTGRES_DSN"]
    schema = "lake_export_" + uuid.uuid4().hex
    with duckdb.connect() as conn:
        conn.execute("LOAD postgres")
        conn.execute(f"ATTACH {module.literal(str(source))} AS source (READ_ONLY)")
        conn.execute(f"ATTACH {module.literal(dsn)} AS pg (TYPE POSTGRES)")
        conn.execute(f"CREATE SCHEMA pg.{schema}")
        tables = conn.sql(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog='source'"
        ).fetchall()
        for (name,) in tables:
            conn.execute(
                f"CREATE TABLE pg.{schema}.{module.identifier(name)} AS SELECT * FROM source.main.{module.identifier(name)}"
            )
        conn.execute(
            f"CALL postgres_execute('pg', 'CREATE INDEX test_snapshot_index ON {schema}.ducklake_snapshot(snapshot_id)')"
        )
    manifest = export_catalog(
        "postgres:" + dsn, tmp_path / "published.duckdb", schema=schema
    )
    assert {t["name"] for t in manifest["tables"]} == {
        "people",
        "empty_table",
        "external",
    }
    with duckdb.connect() as conn:
        conn.execute("LOAD ducklake")
        conn.execute(
            f"ATTACH 'ducklake:{tmp_path}/published.duckdb' AS lake (READ_ONLY)"
        )
        assert conn.sql("SELECT * FROM lake.people").fetchall() == expected
        assert conn.sql("SELECT count(*) FROM lake.snapshots()").fetchone()[0] == 1
    with duckdb.connect() as conn:
        conn.execute("LOAD postgres")
        conn.execute(f"ATTACH {module.literal(dsn)} AS pg (TYPE POSTGRES)")
        conn.execute(f"CALL postgres_execute('pg', 'DROP SCHEMA {schema} CASCADE')")
