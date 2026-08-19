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
use ckan_ingestor_lib::ingestion_service::IngestionService;
use ckan_ingestor_lib::ingestor_outcome::LABEL_DATASTORE;
use common::dummy_s3_ingestor;
use httpmock::{Method::GET, MockServer};

#[test]
fn run_uses_datastore_when_datastore_active() -> Result<()> {
    let server = MockServer::start();
    let id = "datastore-active-id";

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"col1\"}],\"records\":[[\"a\"],[\"b\"]],\"total\":2}");
    });
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "100000")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"col1\"}],\"records\":[],\"total\":2}");
    });

    let conn = duckdb::Connection::open_in_memory()?;
    let outcome = IngestionService::run(
        &conn,
        id,
        "http://example.com/unused.csv",
        "CSV",
        &format!("http://{}/datastore", server.address()),
        true,
        dummy_s3_ingestor(),
    )?;

    assert_eq!(
        outcome.rows_processed, 2,
        "rows must come from the datastore"
    );
    assert!(outcome.datastore_active);
    assert!(
        outcome.labels.contains(&LABEL_DATASTORE.to_string()),
        "expected 'datastore' label when datastore_active is true"
    );
    Ok(())
}

#[test]
fn run_skips_datastore_when_datastore_inactive() -> Result<()> {
    let server = MockServer::start();
    let id = "datastore-inactive-id";

    let csv_path = common::fixture_path("csv_with_bom.csv")
        .to_str()
        .unwrap()
        .to_string();

    let conn = duckdb::Connection::open_in_memory()?;
    let outcome = IngestionService::run(
        &conn,
        id,
        &csv_path,
        "CSV",
        &format!("http://{}/datastore", server.address()),
        false,
        dummy_s3_ingestor(),
    )?;

    assert_eq!(
        outcome.rows_processed, 804,
        "rows must come from CSV fallback"
    );
    assert!(!outcome.datastore_active);
    assert!(!outcome.labels.contains(&LABEL_DATASTORE.to_string()));
    Ok(())
}
