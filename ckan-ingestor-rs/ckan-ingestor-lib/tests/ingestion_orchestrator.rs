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
mod common;
use anyhow::Result;
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::csv_reader::CsvReader;
use ckan_ingestor_lib::datastore_reader::DatastoreReader;
use ckan_ingestor_lib::duckdb_ckan_data_ingestor::DuckdbCkanDataIngestor;
use ckan_ingestor_lib::ingestion_orchestrator::{
    compute_column_labels, compute_labels, IngestionOutcome, LABEL_DATASTORE, LABEL_EMPTY,
};
use common::dummy_s3_ingestor;
use common::fixture_path;
use httpmock::{Method::GET, MockServer};

fn setup_resource_last_update(conn: &duckdb::Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS ckan_resource_last_update \
         (ckan_resource_id VARCHAR, last_modified TIMESTAMP)",
    )
    .unwrap();
}

/// Helper: run a full ingestion for a given resource and return the outcome.
fn run_ingestion_for_test(
    conn: &duckdb::Connection,
    resource: &CkanResource,
    csv_reader: &CsvReader,
    datastore_reader: &DatastoreReader,
) -> Result<IngestionOutcome> {
    let ingestor = DuckdbCkanDataIngestor::new(conn, dummy_s3_ingestor());
    let ingested = ingestor.ingest_ckan_data(resource, csv_reader, datastore_reader, None)?;
    let encoding = csv_reader.last_encoding();
    if !ingested {
        return Ok(IngestionOutcome::new(
            0,
            vec![],
            None,
            None,
            encoding,
            resource.datastore_active,
            None,
            vec![LABEL_EMPTY.to_string()],
        ));
    }
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM \"{}\"", resource.id),
        [],
        |row| row.get(0),
    )?;
    let labels = compute_labels(
        count,
        resource.datastore_active,
        &encoding,
        None, // expected_rows
        None, // resource_size
    );
    // Column labels not computed here (no preview for now)
    Ok(IngestionOutcome::new(
        count,
        vec![],
        None,
        None,
        encoding,
        resource.datastore_active,
        None,
        labels,
    ))
}

#[test]
fn test_compute_labels_empty_resource() -> Result<()> {
    let labels = compute_labels(0, false, &None, None, None);
    assert!(labels.contains(&LABEL_EMPTY.to_string()));
    Ok(())
}

#[test]
fn test_compute_labels_single_row() -> Result<()> {
    let labels = compute_labels(1, false, &None, None, None);
    assert!(!labels.contains(&LABEL_EMPTY.to_string()));
    assert!(labels.contains(&"single-row".to_string()));
    Ok(())
}

#[test]
fn test_compute_labels_datastore() -> Result<()> {
    let labels = compute_labels(100, true, &None, None, None);
    assert!(labels.contains(&LABEL_DATASTORE.to_string()));
    Ok(())
}

#[test]
fn test_compute_labels_encoding() -> Result<()> {
    let labels = compute_labels(100, false, &Some("latin-1".to_string()), None, None);
    assert!(labels.contains(&"encoding:latin-1".to_string()));
    // utf-8 should not generate a label
    let labels_utf8 = compute_labels(100, false, &Some("utf-8".to_string()), None, None);
    assert!(!labels_utf8.iter().any(|l| l.starts_with("encoding:")));
    Ok(())
}

#[test]
fn test_compute_labels_size() -> Result<()> {
    // small
    let labels = compute_labels(100, false, &None, None, Some(500_000));
    assert!(labels.contains(&"size:small".to_string()));
    // medium
    let labels = compute_labels(100, false, &None, None, Some(5_000_000));
    assert!(labels.contains(&"size:medium".to_string()));
    // large
    let labels = compute_labels(100, false, &None, None, Some(2_000_000_000));
    assert!(labels.contains(&"size:large".to_string()));
    Ok(())
}

#[test]
fn test_compute_labels_row_count_mismatch() -> Result<()> {
    let labels = compute_labels(50, false, &None, Some(100), None);
    assert!(labels.contains(&"row-count-mismatch".to_string()));
    // match — no label
    let labels = compute_labels(100, false, &None, Some(100), None);
    assert!(!labels.contains(&"row-count-mismatch".to_string()));
    Ok(())
}

#[test]
fn test_compute_column_count_mismatch() -> Result<()> {
    // This uses compute_column_labels which takes a mutable labels vec
    let mut labels = compute_labels(100, false, &None, None, None);
    compute_column_labels(&mut labels, 3, Some(5));
    assert!(labels.contains(&"column-count-mismatch".to_string()));
    Ok(())
}

#[test]
fn test_compute_column_single_column() -> Result<()> {
    let mut labels = compute_labels(100, false, &None, None, None);
    compute_column_labels(&mut labels, 1, None);
    assert!(labels.contains(&"single-column".to_string()));
    Ok(())
}

#[test]
fn test_full_csv_ingestion_with_labels() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let csv_path = fixture_path("csv_with_bom.csv")
        .to_str()
        .unwrap()
        .to_string();
    let resource = CkanResource {
        id: "label-test-csv".to_string(),
        url: csv_path,
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());

    let outcome = run_ingestion_for_test(&conn, &resource, &csv_reader, &datastore_reader)?;
    assert_eq!(outcome.rows_processed, 804);
    assert!(!outcome.labels.contains(&LABEL_EMPTY.to_string()));
    Ok(())
}

#[test]
fn test_datastore_empty_with_labels_falls_back() -> Result<()> {
    let server = MockServer::start();
    let id = "label-test-datastore-empty";

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"_id\"}],\"records\":[],\"total\":0}");
    });

    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let csv_path = fixture_path("csv_with_bom.csv")
        .to_str()
        .unwrap()
        .to_string();
    let resource = CkanResource {
        id: id.to_string(),
        url: csv_path,
        format: "CSV".to_string(),
        datastore_active: true,
    };

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new(format!("http://{}/datastore", server.address()));

    let outcome = run_ingestion_for_test(&conn, &resource, &csv_reader, &datastore_reader)?;
    // Should fall back to CSV — 804 rows
    assert_eq!(outcome.rows_processed, 804);
    // datastore_active label should be present
    assert!(outcome.labels.contains(&LABEL_DATASTORE.to_string()));
    assert!(!outcome.labels.contains(&LABEL_EMPTY.to_string()));
    Ok(())
}

#[test]
fn test_ingestion_outcome_serialization() -> Result<()> {
    let outcome = IngestionOutcome::new(
        42,
        vec![],
        Some(42),
        Some(1024),
        Some("utf-8".to_string()),
        true,
        Some(3),
        vec!["datastore".to_string()],
    );

    let json = serde_json::to_value(&outcome)?;
    assert_eq!(json["rows_processed"], 42);
    assert_eq!(json["expected_rows"], 42);
    assert_eq!(json["resource_size"], 1024);
    assert_eq!(json["encoding"], "utf-8");
    assert_eq!(json["datastore_active"], true);
    assert_eq!(json["expected_columns"], 3);
    assert_eq!(json["status"], "success");
    Ok(())
}

#[test]
fn test_ingestion_outcome_failure_status() -> Result<()> {
    let outcome = IngestionOutcome::new(
        0,
        vec![],
        None,
        None,
        None,
        false,
        None,
        vec!["empty".to_string()],
    );

    let json = serde_json::to_value(&outcome)?;
    assert_eq!(json["status"], "empty");
    Ok(())
}
