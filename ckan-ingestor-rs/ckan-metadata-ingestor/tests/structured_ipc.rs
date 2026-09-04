// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.
use arrow::datatypes::DataType;
use arrow_ipc::reader::FileReader;
use ckan_ingestor_lib::duckdb_factory::{DuckdbConfig, DuckdbFactory};
use ckan_metadata_ingestor::{DuckdbCkanMetadataIngestor, MetadataSyncCommand, StructuredIpc};
use httpmock::MockServer;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn writes_packages_as_typed_arrow_columns() {
    let ipc = StructuredIpc::from_packages(vec![serde_json::json!({
        "id": "dataset-1",
        "title": "Dataset",
        "notes": 42,
        "resources": [{"id": "resource-1", "url": "https://example.test/data.csv"}]
    })])
    .expect("IPC should be written");

    let package_reader =
        FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let package_batch = package_reader.into_iter().next().unwrap().unwrap();
    assert!(package_batch.column_by_name("id").is_some());
    assert!(package_batch.column_by_name("title").is_some());
    assert!(package_batch.column_by_name("resources").is_none());
    assert_eq!(
        package_batch
            .schema()
            .field_with_name("notes")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );

    let resource_reader =
        FileReader::try_new(File::open(ipc.resource_path().unwrap()).unwrap(), None).unwrap();
    let resource_batch = resource_reader.into_iter().next().unwrap().unwrap();
    assert!(resource_batch.column_by_name("id").is_some());
    assert!(resource_batch.column_by_name("url").is_some());
}

#[test]
fn rejects_fields_not_present_in_the_initial_schema() {
    let packages = (0..101)
        .map(|index| {
            let mut package = serde_json::json!({"id": format!("dataset-{index}")});
            if index == 100 {
                package["new_field"] = serde_json::json!("requires a schema rule");
            }
            package
        })
        .collect::<Vec<_>>();

    let pages = vec![packages[..100].to_vec(), packages[100..].to_vec()];
    assert!(StructuredIpc::from_pages(pages).is_err());
}

#[test]
fn sync_ingests_typed_ipcs_with_read_arrow() {
    let server = MockServer::start();
    let response = serde_json::json!({
        "result": [{
            "id": "dataset-1",
            "name": "dataset",
            "metadata_modified": "2025-01-01",
            "resources": [{
                "id": "resource-1",
                "package_id": "dataset-1",
                "last_modified": "2025-01-01",
                "url": "https://example.test/data.csv"
            }]
        }]
    });
    let mock = server.mock(|when, then| {
        when.method("GET")
            .path("/api/action/current_package_list_with_resources")
            .query_param("limit", "100")
            .query_param("offset", "0");
        then.status(200).json_body(response);
    });
    let temp_dir = tempdir().unwrap();
    let factory = DuckdbFactory::new(DuckdbConfig::for_local_ducklake(
        temp_dir.path().join("catalog.ducklake").to_string_lossy(),
        temp_dir.path().join("data").to_string_lossy(),
    ));
    let conn = factory.open().expect("DuckLake connection should open");
    let ingestor = DuckdbCkanMetadataIngestor::new(&conn);
    let command = MetadataSyncCommand {
        sync_id: "sync-1".into(),
        instance_id: "instance-1".into(),
        instance_name: "Test".into(),
        instance_url: server.url(""),
    };

    let result = ingestor.sync(&command).expect("sync should succeed");

    mock.assert();
    assert_eq!(result.dataset_count, 1);
    assert_eq!(result.resource_count, 1);
    assert_eq!(result.new_datasets, 1);
    assert_eq!(result.new_resources, 1);
    let temporary_stages: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'incoming_metadata'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(temporary_stages, 0);
}
