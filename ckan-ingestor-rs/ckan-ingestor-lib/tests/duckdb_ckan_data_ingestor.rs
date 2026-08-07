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

fn make_csv_resource(id: &str, url: &str) -> CkanResource {
    CkanResource {
        id: id.to_string(),
        url: url.to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    }
}

fn make_datastore_resource(id: &str) -> CkanResource {
    CkanResource {
        id: id.to_string(),
        url: "http://example.com/unused".to_string(),
        format: "CSV".to_string(),
        datastore_active: true,
    }
}

#[test]
fn test_ingest_returns_false_when_all_formats_fail() -> Result<()> {
    // Python: test_ingest_returns_false_when_all_formats_fail
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let resource = CkanResource {
        id: "deadbeef-dead-beef-dead-beefdeadbeef".to_string(),
        url: "http://does-not-matter.because.we.mock".to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    // An HTTP URL will fail since there's no server
    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(
        !result,
        "Expected False when all formats fail, got {result}"
    );

    Ok(())
}

#[test]
fn test_ingest_returns_true_on_success() -> Result<()> {
    // Python: test_ingest_returns_true_on_success
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let csv_path = fixture_path("csv_with_bom.csv")
        .to_str()
        .unwrap()
        .to_string();
    let resource = make_csv_resource("cafe0000-cafe-cafe-cafe-cafe00000000", &csv_path);

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(result, "Expected True on successful ingestion");
    Ok(())
}

#[test]
fn test_datastore_empty_falls_back_to_csv() -> Result<()> {
    // Python: test_datastore_empty_falls_back_to_csv
    let server = MockServer::start();
    let id = "5afe0000-5afe-5afe-5afe-5afe00000000";

    // Datastore returns empty records
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
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(
        result,
        "Expected fallback to CSV when datastore is empty, got {result}"
    );

    // Verify table was created with CSV data (804 rows from csv_with_bom.csv)
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{id}\""), [], |row| {
        row.get(0)
    })?;
    assert_eq!(count, 804, "Should have 804 rows from CSV fallback");

    Ok(())
}

#[test]
fn test_ingest_datastore_active_creates_table() -> Result<()> {
    // Python: equivalent — verifies datastore ingestion creates the table + updates last_update
    let server = MockServer::start();
    let id = "a6b97d48-a9fb-4991-9893-d920ffb19b90";
    let body = std::fs::read_to_string(fixture_path(&format!("{id}.json")))?;

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body);
    });
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "100000")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"_id\"}],\"records\":[]}");
    });

    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new(format!("http://{}/datastore", server.address()));
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let resource = make_datastore_resource(id);
    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(result, "Datastore ingestion should succeed");

    // Verify table was created
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{id}\""), [], |row| {
        row.get(0)
    })?;
    assert!(count > 0, "Table should have rows");

    // Verify ckan_resource_last_update was updated
    let update_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM ckan_resource_last_update WHERE ckan_resource_id = ?",
        [id],
        |row| row.get(0),
    )?;
    assert_eq!(update_count, 1);

    Ok(())
}

#[test]
fn test_csv_ingestion_creates_table_and_tracks_encoding() -> Result<()> {
    // Python: equivalent — verifies CSV ingestion creates the table and tracks encoding
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let csv_path = fixture_path("csv_with_bom.csv")
        .to_str()
        .unwrap()
        .to_string();
    let resource = make_csv_resource("00000000-0000-0000-0000-ffff00000001", &csv_path);

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(result, "CSV ingestion should succeed");

    // Verify table was created
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM \"00000000-0000-0000-0000-ffff00000001\"",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 804, "Should have 804 rows from csv_with_bom.csv");

    // Verify encoding was tracked
    let encoding = csv_reader.last_encoding();
    assert!(encoding.is_some(), "Encoding should be tracked");
    assert_eq!(encoding.unwrap(), "utf-8");

    Ok(())
}

#[test]
fn test_csv_fallback_on_http_failure() -> Result<()> {
    // Python: the except block catches duckdb.IOException etc and falls back to
    // next format. Here the CSV URL is a local file that doesn't exist, but
    // the resource has datastore_active=false, so after CSV fails there's no
    // fallback and it should return false.
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let resource = CkanResource {
        id: "fail-csv-no-fallback".to_string(),
        url: "/nonexistent/file.csv".to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(
        !result,
        "Expected False when CSV fails and no fallback available"
    );

    Ok(())
}

#[test]
fn test_json_format_direct_query() -> Result<()> {
    // Python: json format uses read_json() directly
    // We'll use a local JSON file via the fixture
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let json_path = fixture_path("a6b97d48-a9fb-4991-9893-d920ffb19b90.json")
        .to_str()
        .unwrap()
        .to_string();

    let resource = CkanResource {
        id: "json-resource-001".to_string(),
        url: json_path,
        format: "JSON".to_string(),
        datastore_active: false,
    };

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(result, "JSON ingestion should succeed");

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM \"json-resource-001\"", [], |row| {
        row.get(0)
    })?;
    assert_eq!(count, 1, "Should have 1 row from the JSON fixture");

    Ok(())
}

#[test]
fn test_unsupported_format_returns_false() -> Result<()> {
    // Python: unsupported format logs error and returns False
    let conn = duckdb::Connection::open_in_memory()?;
    setup_resource_last_update(&conn);

    let resource = CkanResource {
        id: "unsupported-format-id".to_string(),
        url: "http://example.com/file.xlsx".to_string(),
        format: "XLSX".to_string(),
        datastore_active: false,
    };

    let csv_reader = CsvReader::new(&conn);
    let datastore_reader = DatastoreReader::new("http://localhost/datastore".to_string());
    let ingestor = DuckdbCkanDataIngestor::new(&conn, dummy_s3_ingestor());

    let result = ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;
    assert!(!result, "Unsupported format should return false");

    Ok(())
}
