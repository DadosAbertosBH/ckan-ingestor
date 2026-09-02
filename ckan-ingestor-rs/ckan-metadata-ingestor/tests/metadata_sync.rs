// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_metadata_ingestor::{DuckdbCkanMetadataIngestor, MetadataSyncCommand};
use duckdb::Connection;
use serde_json::{Value, json};

fn command() -> MetadataSyncCommand {
    MetadataSyncCommand {
        sync_id: "sync-1".into(),
        instance_id: "instance-1".into(),
        instance_name: "Test".into(),
        instance_url: "https://ckan.example".into(),
    }
}

fn package(
    dataset_id: &str,
    dataset_modified: &str,
    resource_id: &str,
    resource_modified: &str,
) -> Value {
    json!({
        "id": dataset_id,
        "name": dataset_id,
        "metadata_modified": dataset_modified,
        "resources": [{
            "id": resource_id,
            "name": resource_id,
            "package_id": dataset_id,
            "last_modified": resource_modified,
            "url": format!("https://example.test/{resource_id}.csv")
        }]
    })
}

#[test]
fn identical_second_sync_reports_no_changes() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let packages = vec![package("ds-1", "2024-01-01", "res-1", "2024-01-01")];
    ingestor
        .ingest_packages(&command(), packages.clone())
        .unwrap();

    let result = ingestor.ingest_packages(&command(), packages).unwrap();

    assert_eq!(result.new_datasets, 0);
    assert_eq!(result.new_resources, 0);
    assert_eq!(result.updated_datasets, 0);
    assert_eq!(result.updated_resources, 0);
    let dataset_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM ckan_dataset", [], |row| row.get(0))
        .unwrap();
    assert_eq!(dataset_rows, 1);
}

#[test]
fn sync_initializes_resource_update_history_before_jobs_are_enqueued() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);

    let _ = ingestor.sync(&command());

    let table_exists: bool = conn
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_name = 'ckan_resource_last_update')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(table_exists);
}

#[test]
fn dataset_and_resource_updates_are_counted() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2024-01-01", "res-1", "2024-01-01")],
        )
        .unwrap();

    let result = ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2025-01-01", "res-1", "2025-01-01")],
        )
        .unwrap();

    assert_eq!(result.new_datasets, 0);
    assert_eq!(result.new_resources, 0);
    assert_eq!(result.updated_datasets, 1);
    assert_eq!(result.updated_resources, 1);
}

#[test]
fn resource_update_marks_unchanged_dataset_as_updated() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2024-01-01", "res-1", "2024-01-01")],
        )
        .unwrap();

    let result = ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2024-01-01", "res-1", "2025-01-01")],
        )
        .unwrap();

    assert_eq!(result.updated_datasets, 1);
    assert_eq!(result.updated_resources, 1);
}

#[test]
fn mixed_new_and_updated_rows_are_counted_separately() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2024-01-01", "res-1", "2024-01-01")],
        )
        .unwrap();

    let result = ingestor
        .ingest_packages(
            &command(),
            vec![
                package("ds-1", "2025-01-01", "res-1", "2025-01-01"),
                package("ds-2", "2025-01-01", "res-2", "2025-01-01"),
            ],
        )
        .unwrap();

    assert_eq!(result.new_datasets, 1);
    assert_eq!(result.new_resources, 1);
    assert_eq!(result.updated_datasets, 1);
    assert_eq!(result.updated_resources, 1);
}

#[test]
fn new_columns_are_added_to_existing_tables() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2024-01-01", "res-1", "2024-01-01")],
        )
        .unwrap();
    let mut new_package = package("ds-2", "2025-01-01", "res-2", "2025-01-01");
    new_package
        .as_object_mut()
        .unwrap()
        .insert("notes".into(), json!("new field"));

    ingestor
        .ingest_packages(&command(), vec![new_package])
        .unwrap();

    let has_notes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('ckan_dataset') WHERE name = 'notes'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(has_notes, 1);
}

#[test]
fn scalar_type_conflicts_are_promoted_to_varchar() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let mut first = package("ds-1", "2024-01-01", "res-1", "2024-01-01");
    first
        .as_object_mut()
        .unwrap()
        .insert("notes".into(), json!(42));
    ingestor.ingest_packages(&command(), vec![first]).unwrap();
    let mut second = package("ds-2", "2025-01-01", "res-2", "2025-01-01");
    second
        .as_object_mut()
        .unwrap()
        .insert("notes".into(), json!("description"));

    ingestor.ingest_packages(&command(), vec![second]).unwrap();

    let data_type: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('ckan_dataset') WHERE name = 'notes'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(data_type, "VARCHAR");
}

#[test]
fn rich_extras_are_ignored_and_legacy_json_column_is_preserved() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE ckan_dataset AS SELECT 'old' AS id, 'old' AS name, '2024-01-01' AS metadata_modified, '[]'::JSON AS extras").unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let mut next = package("ds-2", "2025-01-01", "res-2", "2025-01-01");
    next.as_object_mut()
        .unwrap()
        .insert("extras".into(), json!([{"key": "tema", "value": "saude"}]));

    let result = ingestor.ingest_packages(&command(), vec![next]).unwrap();

    let data_type: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('ckan_dataset') WHERE name = 'extras'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(data_type, "JSON");
    assert_eq!(result.new_datasets, 1);
}

#[test]
fn missing_fields_and_empty_lists_are_accepted_across_rows() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let mut first = package("ds-1", "2024-01-01", "res-1", "2024-01-01");
    first
        .as_object_mut()
        .unwrap()
        .insert("groups".into(), json!([]));
    let mut second = package("ds-2", "2024-01-01", "res-2", "2024-01-01");
    second
        .as_object_mut()
        .unwrap()
        .insert("groups".into(), json!([{"name": "group"}]));
    second
        .as_object_mut()
        .unwrap()
        .insert("optional".into(), json!("present"));

    let result = ingestor
        .ingest_packages(&command(), vec![first, second])
        .unwrap();

    assert_eq!(result.new_datasets, 2);
    let null_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM ckan_dataset WHERE optional IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(null_count, 1);
}

#[test]
fn columns_containing_only_empty_or_null_lists_are_dropped() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let mut first = package("ds-1", "2024-01-01", "res-1", "2024-01-01");
    first
        .as_object_mut()
        .unwrap()
        .insert("groups".into(), json!([]));
    let mut second = package("ds-2", "2024-01-01", "res-2", "2024-01-01");
    second
        .as_object_mut()
        .unwrap()
        .insert("groups".into(), Value::Null);

    ingestor
        .ingest_packages(&command(), vec![first, second])
        .unwrap();

    let group_columns: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('ckan_dataset') WHERE name = 'groups'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(group_columns, 0);
}

#[test]
fn incompatible_non_empty_list_shapes_are_rejected() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let mut first = package("ds-1", "2024-01-01", "res-1", "2024-01-01");
    first
        .as_object_mut()
        .unwrap()
        .insert("groups".into(), json!([{"name": "group"}]));
    let mut second = package("ds-2", "2024-01-01", "res-2", "2024-01-01");
    second
        .as_object_mut()
        .unwrap()
        .insert("groups".into(), json!(["group"]));

    let error = ingestor
        .ingest_packages(&command(), vec![first, second])
        .unwrap_err();

    assert!(error.to_string().contains("incompatible list shape"));
}

#[test]
fn transaction_rolls_back_dataset_changes_when_resource_merge_fails() {
    let conn = Connection::open_in_memory().unwrap();
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    ingestor
        .ingest_packages(
            &command(),
            vec![package("ds-1", "2024-01-01", "res-1", "2024-01-01")],
        )
        .unwrap();
    let mut invalid = package("ds-2", "2025-01-01", "res-2", "2025-01-01");
    invalid["resources"][0]
        .as_object_mut()
        .unwrap()
        .remove("last_modified");

    assert!(ingestor.ingest_packages(&command(), vec![invalid]).is_err());
    let dataset_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ckan_dataset", [], |row| row.get(0))
        .unwrap();
    assert_eq!(dataset_count, 1);
}
