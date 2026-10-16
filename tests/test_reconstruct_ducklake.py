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
"""Tests for the DuckLake catalog reconstruction runner."""

import importlib.util
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "analytics" / "reconstruct_ducklake.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("reconstruct_ducklake", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


reconstruct = _load_module()


def test_build_setup_attaches_without_data_inlining():
    setup = reconstruct.build_setup("pw", "ak", "sk")

    assert "DATA_INLINING_ROW_LIMIT" not in setup
    assert "AS lake (DATA_PATH 's3://public-datasets');" in setup


def test_build_setup_uses_bare_s3_endpoint():
    setup = reconstruct.build_setup("pw", "ak", "sk")

    # Uses a shared S3 secret (visible to the extension's internal connection),
    # not per-connection `SET s3_*` settings.
    assert "CREATE SECRET r2 (TYPE S3" in setup
    assert (
        "ENDPOINT 'd8915a6c9b1e24b2ded7d9cd72318cdd.r2.cloudflarestorage.com'" in setup
    )
    assert "SET s3_endpoint" not in setup
    assert "SET s3_access_key_id" not in setup


def test_build_setup_disables_geoparquet_conversion():
    setup = reconstruct.build_setup("pw", "ak", "sk")

    # GeoParquet v1 files carry a raw BYTE_ARRAY geometry column. Disabling the
    # conversion keeps read_parquet's schema (BLOB) aligned with the footer the
    # extension reads for ducklake_add_data_files, which rejects GEOMETRY.
    assert "SET enable_geoparquet_conversion = false;" in setup


def test_build_setup_accepts_custom_catalog_host():
    custom = reconstruct.build_setup("pw", "ak", "sk", host="ingestor-orchestrator-postgres-rw")
    default = reconstruct.build_setup("pw", "ak", "sk")

    assert "host=ingestor-orchestrator-postgres-rw" in custom
    assert (
        "host=orchestrator-ingestor-orchestrator-postgres-tailscale.tail49b842.ts.net"
        in default
    )


def test_resolve_credentials_prefers_env_over_kubectl(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_PG_PASSWORD", "envpw")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH__ACCESS_KEY_ID", "envak")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY", "envsk")

    def _fail(*_args, **_kwargs):
        raise AssertionError("kubectl should not be called when env vars are set")

    monkeypatch.setattr(reconstruct, "read_secret", _fail)

    assert reconstruct.resolve_credentials() == ("envpw", "envak", "envsk")


def test_create_table_statement_prepends_create_for_call():
    call = (
        "CALL ducklake_add_data_files('lake', "
        "'0000f506-377b-4005-88fa-5e217c78aa03', "
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet');"
    )

    assert reconstruct.create_table_statement(call) == (
        'CREATE TABLE IF NOT EXISTS lake.main."0000f506-377b-4005-88fa-5e217c78aa03" '
        "AS SELECT * FROM read_parquet("
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet') LIMIT 0;"
    )


def test_create_table_statement_rejects_malformed_call():
    try:
        reconstruct.create_table_statement("SELECT 1;")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for a non-CALL statement")


def test_drop_table_statement_uses_lake_main_qualified_name():
    call = (
        "CALL ducklake_add_data_files('lake', "
        "'0000f506-377b-4005-88fa-5e217c78aa03', "
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet');"
    )

    assert reconstruct.drop_table_statement(call) == (
        'DROP TABLE IF EXISTS lake.main."0000f506-377b-4005-88fa-5e217c78aa03";'
    )


def test_expand_statement_emits_drop_create_call_in_order():
    call = (
        "CALL ducklake_add_data_files('lake', "
        "'0000f506-377b-4005-88fa-5e217c78aa03', "
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet');"
    )

    assert reconstruct.expand_statement(call) == [
        'DROP TABLE IF EXISTS lake.main."0000f506-377b-4005-88fa-5e217c78aa03";',
        'CREATE TABLE IF NOT EXISTS lake.main."0000f506-377b-4005-88fa-5e217c78aa03" '
        "AS SELECT * FROM read_parquet("
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet') LIMIT 0;",
        'CALL ducklake_add_data_files(\'lake\', '
        "'0000f506-377b-4005-88fa-5e217c78aa03', "
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet', "
        'allow_missing => true, ignore_extra_columns => true);',
    ]


def test_add_data_files_statement_allows_missing_and_ignores_extra_columns():
    call = (
        "CALL ducklake_add_data_files('lake', "
        "'fa123cc3-2bc7-4e0a-b5df-056d69ea7ca8', "
        "'s3://public-datasets/fa123cc3-2bc7-4e0a-b5df-056d69ea7ca8/"
        "ducklake_2024-01-22T13:58:29.535511.parquet');"
    )

    statement = reconstruct.add_data_files_statement(call)

    assert statement == (
        "CALL ducklake_add_data_files('lake', "
        "'fa123cc3-2bc7-4e0a-b5df-056d69ea7ca8', "
        "'s3://public-datasets/fa123cc3-2bc7-4e0a-b5df-056d69ea7ca8/"
        "ducklake_2024-01-22T13:58:29.535511.parquet', "
        "allow_missing => true, ignore_extra_columns => true);"
    )
