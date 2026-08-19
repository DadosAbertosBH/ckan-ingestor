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
use ckan_ingestor_lib::readers::datastore_reader::DatastoreReader;
use common::fixture_path;
use httpmock::{Method::GET, MockServer};

#[test]
fn read_datastore() -> Result<()> {
    let server = MockServer::start();
    let id = "a6b97d48-a9fb-4991-9893-d920ffb19b90";
    let body = std::fs::read_to_string(fixture_path(&format!("{id}.json")))?;
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "0");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"_id\"},{\"id\":\"ORGAO/ENTIDADE\"},{\"id\":\"TOTAL\"}],\"records\":[],\"total\":1}");
    });
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body.clone());
    });
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "100000")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"_id\"}],\"records\":[],\"total\":1}");
    });

    let reader = DatastoreReader::new(format!("http://{}/datastore", server.address()));

    let resource = CkanResource {
        id: id.to_string(),
        url: fixture_path("non_latin1_and_non_utf8.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: true,
    };

    let result = reader.read_batches(&resource)?;
    assert!(!result.data.is_empty());
    assert_eq!(result.expected_rows, Some(1));
    assert_eq!(result.expected_columns, Some(3));
    Ok(())
}

#[test]
fn read_invalid_json() -> Result<()> {
    let server = MockServer::start();
    let id = "e3bce367-2e62-41c2-840f-b1df6255e7e5";
    let body = std::fs::read_to_string(fixture_path(&format!("{id}.json")))?;
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body.clone());
    });
    let reader = DatastoreReader::new(format!("http://{}", server.address()));

    let resource = CkanResource {
        id: id.to_string(),
        url: fixture_path("non_latin1_and_non_utf8.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: true,
    };

    let result = reader.read_batches(&resource);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn empty_datastore_returns_empty_vec() -> Result<()> {
    let server = MockServer::start();
    let id = "empty-datastore-id";

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

    let reader = DatastoreReader::new(format!("http://{}/datastore", server.address()));

    let resource = CkanResource {
        id: id.to_string(),
        url: "http://example.com/data.csv".to_string(),
        format: "CSV".to_string(),
        datastore_active: true,
    };

    let result = reader.read_batches(&resource)?;
    assert!(
        result.data.is_empty(),
        "Empty datastore should return empty vec, not Err"
    );
    Ok(())
}

#[test]
fn get_row_and_column_count_returns_datastore_metadata() -> Result<()> {
    let server = MockServer::start();
    let id = "test-get-total-id";

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "0");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"col1\"},{\"id\":\"col2\"},{\"id\":\"col3\"}],\"records\":[],\"total\":42}");
    });

    let reader = DatastoreReader::new(format!("http://{}/datastore", server.address()));

    let (rows, columns) = reader.get_row_and_column_count(id)?;
    assert_eq!(rows, 42);
    assert_eq!(columns, 3);

    Ok(())
}
