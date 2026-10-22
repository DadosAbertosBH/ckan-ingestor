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
from copy import deepcopy
import importlib.util
import io
import json
from pathlib import Path

import duckdb
import pytest
from botocore.response import StreamingBody

spec = importlib.util.spec_from_file_location(
    "catalog_ingestion",
    Path(__file__).resolve().parents[1] / "analytics/catalog_ingestion.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ingest_snapshot = module.ingest_snapshot
replace_collection = module.replace_collection


@pytest.fixture
def catalog(tmp_path):
    with duckdb.connect() as conn:
        conn.execute("LOAD ducklake")
        conn.execute(
            f"ATTACH 'ducklake:{tmp_path}/catalog.duckdb' AS lake (DATA_PATH '{tmp_path}/data')"
        )
        yield conn


def snapshot(source="a", sync="one", at="2026-10-22T10:00:00Z"):
    return {
        "source": {
            "id": source,
            "title": "Portal " + source,
            "type": "ckan",
            "url": "https://example.test",
        },
        "sync_id": sync,
        "collected_at": at,
        "resources": [
            {
                "id": "p",
                "metadata_collection": "ckan_packages",
                "kind": "collection",
                "title": "Despesas",
                "metadata": {"extras": {"custom": 42}},
            },
            {
                "id": "r",
                "metadata_collection": "ckan_resources",
                "parent_resource_id": "p",
                "kind": "file",
                "title": "2026.csv",
                "content_url": "https://example.test/2026.csv",
                "metadata": {"unusual": [1, "a"]},
            },
        ],
    }


def test_snapshots_preserve_raw_json_and_scope_current_state_by_source(catalog):
    ingest_snapshot(catalog, snapshot())
    ingest_snapshot(catalog, snapshot("b"))
    assert catalog.sql("SELECT count(*) FROM lake.catalog_resources").fetchone()[0] == 4
    assert (
        catalog.sql(
            "SELECT json_extract(metadata, '$.extras.custom') FROM lake.ckan_packages WHERE source_id='a'"
        ).fetchone()[0]
        == "42"
    )
    assert (
        catalog.sql(
            "SELECT json_extract(metadata, '$.unusual[1]') FROM lake.ckan_resources WHERE source_id='b'"
        ).fetchone()[0]
        == '"a"'
    )
    result = catalog.execute("SELECT * FROM lake.ckan_packages WHERE source_id='a'")
    row = dict(
        zip(
            [column[0] for column in result.description], result.fetchone(), strict=True
        )
    )
    assert row.get("sync_id") == "one"


def test_history_tracks_changes_removals_and_restoration_idempotently(catalog):
    initial = snapshot()
    ingest_snapshot(catalog, initial)
    ingest_snapshot(catalog, initial)
    assert (
        catalog.sql(
            "SELECT count(*) FROM lake.catalog_history WHERE collection_name='catalog_resources'"
        ).fetchone()[0]
        == 2
    )
    changed = snapshot(sync="two", at="2026-10-23T10:00:00Z")
    changed["resources"][0]["title"] = "Despesas públicas"
    changed["resources"].pop()
    ingest_snapshot(catalog, changed)
    assert catalog.sql(
        "SELECT deleted FROM lake.catalog_resources WHERE id='r'"
    ).fetchone()[0]
    assert catalog.sql("SELECT count(*) FROM lake.ckan_resources").fetchone()[0] == 0
    assert (
        catalog.sql(
            "SELECT count(*) FROM lake.catalog_history WHERE collection_name='catalog_resources'"
        ).fetchone()[0]
        == 4
    )
    restored = snapshot(sync="three", at="2026-10-24T10:00:00Z")
    ingest_snapshot(catalog, restored)
    assert not catalog.sql(
        "SELECT deleted FROM lake.catalog_resources WHERE id='r'"
    ).fetchone()[0]
    ingest_snapshot(catalog, initial)
    assert (
        catalog.sql(
            "SELECT sync_id FROM lake.catalog_imports ORDER BY collected_at DESC LIMIT 1"
        ).fetchone()[0]
        == "three"
    )


@pytest.mark.parametrize("mutation", ["cycle", "missing", "duplicate", "foreign"])
def test_invalid_snapshot_preserves_committed_state(catalog, mutation):
    ingest_snapshot(catalog, snapshot())
    bad = snapshot(sync="bad", at="2026-10-23T10:00:00Z")
    if mutation == "cycle":
        bad["resources"][0]["parent_resource_id"] = "r"
    elif mutation == "missing":
        bad["resources"][1]["parent_resource_id"] = "missing"
    elif mutation == "duplicate":
        bad["resources"].append(deepcopy(bad["resources"][0]))
    else:
        bad["resources"][0]["source_id"] = "b"
    with pytest.raises(ValueError):
        ingest_snapshot(catalog, bad)
    assert (
        catalog.sql(
            "SELECT count(*) FROM lake.catalog_resources WHERE NOT deleted"
        ).fetchone()[0]
        == 2
    )
    assert catalog.sql("SELECT count(*) FROM lake.catalog_imports").fetchone()[0] == 1


def test_empty_complete_snapshot_removes_only_its_source(catalog):
    ingest_snapshot(catalog, snapshot())
    ingest_snapshot(catalog, snapshot("b"))
    empty = snapshot(sync="empty", at="2026-10-23T10:00:00Z")
    empty["resources"] = []
    ingest_snapshot(catalog, empty)
    assert (
        catalog.sql(
            "SELECT count(*) FROM lake.catalog_resources WHERE source_id='a' AND deleted"
        ).fetchone()[0]
        == 2
    )
    assert (
        catalog.sql(
            "SELECT count(*) FROM lake.catalog_resources WHERE source_id='b' AND NOT deleted"
        ).fetchone()[0]
        == 2
    )


def test_collection_replacement_supports_arbitrary_json_datasets(catalog):
    columns = {"source_id": "VARCHAR", "id": "VARCHAR", "metadata": "JSON"}
    replace_collection(
        catalog,
        "custom_inventory",
        "a",
        [{"id": "x", "metadata": {"nested": [True]}}],
        columns,
    )
    replace_collection(
        catalog,
        "custom_inventory",
        "b",
        [{"id": "x", "metadata": {"other": "value"}}],
        columns,
    )
    replace_collection(catalog, "custom_inventory", "a", [], columns)
    assert catalog.sql(
        "SELECT source_id, metadata->>'other' FROM lake.custom_inventory"
    ).fetchall() == [("b", "value")]


def test_adapter_declares_its_metadata_collections_for_generic_ingestion(catalog):
    replace_collection(
        catalog, "custom_inventory", "a", [{"id": "old"}], module.RAW_COLUMNS
    )
    document = snapshot()
    document["source"]["type"] = "custom"
    document["resources"][1]["metadata_collection"] = "custom_inventory"
    ingest_snapshot(catalog, document)
    assert catalog.sql("SELECT id FROM lake.custom_inventory").fetchall() == [("r",)]
    empty = deepcopy(document)
    empty.update(sync_id="empty", collected_at="2026-10-23T10:00:00Z", resources=[])
    ingest_snapshot(catalog, empty)
    assert catalog.sql("SELECT count(*) FROM lake.custom_inventory").fetchone()[0] == 0


def test_links_recover_tables_by_their_globally_unique_resource_id(catalog):
    assert module.physical_resource_id("r") == "r"
    ingest_snapshot(catalog, snapshot())
    catalog.execute("CREATE TABLE lake.r(value INTEGER)")
    module.refresh_table_links(catalog)
    assert (
        catalog.sql(
            "SELECT status FROM lake.catalog_resources WHERE source_id='a' AND id='r'"
        ).fetchone()[0]
        == "completed"
    )
    assert catalog.sql(
        "SELECT source_id,resource_id,table_name FROM lake.catalog_tables"
    ).fetchall() == [("a", "r", "r")]
    other = snapshot("b")
    other["resources"][1]["id"] = "other-resource"
    ingest_snapshot(catalog, other)
    catalog.execute('CREATE TABLE lake."other-resource"(value INTEGER)')
    module.refresh_table_links(catalog)
    assert catalog.sql(
        "SELECT source_id,resource_id,table_name FROM lake.catalog_tables"
    ).fetchall() == [("a", "r", "r"), ("b", "other-resource", "other-resource")]


def test_result_updates_status_without_hiding_last_successful_table(catalog):
    document = snapshot()
    document["resources"][1]["source_version"] = "v2"
    ingest_snapshot(catalog, document)
    for version, state, at in [
        ("v1", "FAILED", 1),
        ("v2", "SUCCESS", 2),
        ("v2", "FAILED", 3),
    ]:
        module.ingest_result(
            catalog,
            {
                "job_id": str(at),
                "instance_id": "a",
                "resource_id": "r",
                "source_version": version,
                "status": state,
                "recorded_at": at,
            },
        )
    assert (
        catalog.sql(
            "SELECT status FROM lake.catalog_resources WHERE id='r'"
        ).fetchone()[0]
        == "failed"
    )
    assert catalog.sql("SELECT count(*) FROM lake.catalog_results").fetchone()[0] == 3
    module.ingest_result(
        catalog,
        {
            "job_id": "3",
            "instance_id": "a",
            "resource_id": "r",
            "source_version": "v2",
            "status": "FAILED",
            "recorded_at": 3,
        },
    )
    assert catalog.sql("SELECT count(*) FROM lake.catalog_results").fetchone()[0] == 3


class Inbox:
    def __init__(self, documents):
        self.documents = documents
        self.reads = []

    def get_paginator(self, _):
        return self

    def paginate(self, Bucket, Prefix):
        yield {
            "Contents": [
                {"Key": key} for key in self.documents if key.startswith(Prefix)
            ]
        }

    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        body = self.documents[Key]
        return {"Body": StreamingBody(io.BytesIO(body), len(body))}


def test_inbox_imports_in_collection_order_and_skips_committed_artifacts(catalog):
    documents = {}
    for document in [snapshot(sync="two", at="2026-10-23T10:00:00Z"), snapshot()]:
        header = {key: value for key, value in document.items() if key != "resources"}
        payload = "\n".join(
            json.dumps(record)
            for record in [dict(header, version=1), *document["resources"]]
        )
        documents[f"catalog-inbox/a/{document['sync_id']}.jsonl"] = payload.encode()
    inbox = Inbox(documents)
    module.import_inbox(catalog, inbox, "lake")
    assert catalog.sql(
        "SELECT sync_id FROM lake.catalog_imports ORDER BY collected_at"
    ).fetchall() == [("one",), ("two",)]
    inbox.reads.clear()
    module.import_inbox(catalog, inbox, "lake")
    assert inbox.reads == []


def test_inbox_skips_committed_outcomes_and_applies_them_when_discovery_arrives(
    catalog,
):
    event_key = "catalog-results/event.json"
    inbox = Inbox(
        {
            event_key: json.dumps(
                {
                    "job_id": "job",
                    "instance_id": "a",
                    "resource_id": "r",
                    "source_version": "v2",
                    "status": "FAILED",
                    "recorded_at": 1,
                }
            ).encode()
        }
    )
    module.import_inbox(catalog, inbox, "lake")
    document = snapshot()
    document["resources"][1]["source_version"] = "v2"
    header = {key: value for key, value in document.items() if key != "resources"}
    inbox.documents["catalog-inbox/a/one.jsonl"] = "\n".join(
        json.dumps(record)
        for record in [dict(header, version=1), *document["resources"]]
    ).encode()
    inbox.reads.clear()
    module.import_inbox(catalog, inbox, "lake")
    assert event_key not in inbox.reads
    assert (
        catalog.sql(
            "SELECT status FROM lake.catalog_resources WHERE id='r'"
        ).fetchone()[0]
        == "failed"
    )
