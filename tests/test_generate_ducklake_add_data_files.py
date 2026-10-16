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
"""Tests for the DuckLake add-data-files SQL generator."""

import importlib.util
import sys
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "analytics"
    / "generate_ducklake_add_data_files.py"
)

UUID = "0000f506-377b-4005-88fa-5e217c78aa03"
UUID7 = "019b0f60-5f14-7ffe-894a-d40d2cf1410b"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "generate_ducklake_add_data_files", MODULE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


generate = _load_module()


def test_classify_parquet_separates_latest_from_orphans():
    keys = [
        "a/ducklake_2025-01-01T00:00:00.000000.parquet",
        "a/ducklake_2025-03-01T00:00:00.000000.parquet",
        "b/ducklake_2025-01-01T00:00:00.000000.parquet",
    ]

    latest, orphans = generate.classify_parquet(keys)

    assert latest == {
        "a": "a/ducklake_2025-03-01T00:00:00.000000.parquet",
        "b": "b/ducklake_2025-01-01T00:00:00.000000.parquet",
    }
    assert orphans == ["a/ducklake_2025-01-01T00:00:00.000000.parquet"]


def test_classify_parquet_flags_legacy_and_old_convention_as_orphans():
    keys = [
        "a/ducklake_2025-01-01T00:00:00.000000.parquet",
        "a/ducklake-7822f5bd-3eac-4a58-98fa-9a99e6956163.parquet",
        f"main/{UUID}/ducklake-{UUID7}.parquet",
    ]

    latest, orphans = generate.classify_parquet(keys)

    assert latest == {"a": "a/ducklake_2025-01-01T00:00:00.000000.parquet"}
    assert sorted(orphans) == sorted(
        [
            "a/ducklake-7822f5bd-3eac-4a58-98fa-9a99e6956163.parquet",
            f"main/{UUID}/ducklake-{UUID7}.parquet",
        ]
    )


def test_classify_parquet_ignores_metadata_tables():
    keys = [
        f"main/ckan_resource/ducklake-{UUID7}.parquet",
        f"main/ckan_dataset/ducklake-{UUID7}.parquet",
        f"main/ckan_resource_last_update/ducklake-{UUID7}.parquet",
    ]

    latest, orphans = generate.classify_parquet(keys)

    assert latest == {}
    assert orphans == []


def test_classify_parquet_skips_non_parquet_and_root_objects():
    keys = [
        "/docs/x.pdf",
        "no_folder.parquet",
        "a/notes.txt",
        "a/",
    ]

    latest, orphans = generate.classify_parquet(keys)

    assert latest == {}
    assert orphans == []


def test_render_statement_uses_catalog_lake_and_full_s3_path():
    statement = generate.render_statement(
        "public-datasets",
        "0000f506-377b-4005-88fa-5e217c78aa03/ducklake_2025-03-15T03:04:17.669458.parquet",
    )

    assert statement == (
        "CALL ducklake_add_data_files('lake', "
        "'0000f506-377b-4005-88fa-5e217c78aa03', "
        "'s3://public-datasets/0000f506-377b-4005-88fa-5e217c78aa03/"
        "ducklake_2025-03-15T03:04:17.669458.parquet');"
    )


def test_render_statement_escapes_single_quotes():
    statement = generate.render_statement("bucket", "o'brien/ducklake_v1.parquet")

    assert "'o''brien'" in statement


def test_chunked_splits_into_requested_sizes():
    assert list(generate.chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_write_batches_emits_one_statement_per_line(tmp_path):
    statements = [
        generate.render_statement("bucket", "a/ducklake_v1.parquet"),
        generate.render_statement("bucket", "b/ducklake_v2.parquet"),
    ]

    written = generate.write_batches(statements, tmp_path, "add_data_files", 1)

    assert written == 2
    first = (tmp_path / "add_data_files_0001.sql").read_text().splitlines()
    second = (tmp_path / "add_data_files_0002.sql").read_text().splitlines()
    assert first == [statements[0]]
    assert second == [statements[1]]


def test_write_orphans_emits_full_s3_uris(tmp_path):
    generate.write_orphans(
        ["a/ducklake_v1.parquet", "b/ducklake_v2.parquet"],
        "bucket",
        tmp_path,
        "orphans.txt",
    )

    assert (tmp_path / "orphans.txt").read_text() == (
        "s3://bucket/a/ducklake_v1.parquet\ns3://bucket/b/ducklake_v2.parquet\n"
    )
