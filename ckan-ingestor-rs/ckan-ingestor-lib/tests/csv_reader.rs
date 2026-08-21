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
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use common::fixture_path;
use flate2::{write::GzEncoder, Compression};
use httpmock::{Method::GET, MockServer};
use std::io::Write;
use std::time::Duration;

fn test_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(600))
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        )
        .build()
        .unwrap()
}

#[test]
fn parse_latin_encoded_csv() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_latin_encode.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    // Python equivalent: test_parse_latin_encoded_csv_file
    // Just verifies it doesn't error. DuckDB with encoding='latin-1'
    // may fall through to PyArrow fallback for semicolon-delimited files.
    let result = reader.read(&resource)?;
    assert!(result.rows_processed > 0, "Should parse at least 1 row");
    assert!(result.encoding.is_some());
    Ok(())
}

#[test]
fn parse_non_latin_and_non_utf8() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("non_latin1_and_non_utf8.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 2);
    // This file uses latin-1 encoding that only works after the PyArrow fallback
    // with semicolon delimiter
    assert!(result.encoding.is_some());
    Ok(())
}

#[test]
fn csv_with_bom() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };
    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 804);
    assert_eq!(result.encoding.as_deref(), Some("utf-8"));
    Ok(())
}

#[test]
fn reads_csv_data_with_rows() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 804);

    Ok(())
}

#[test]
fn parse_remote_gzip_csv() -> Result<()> {
    let server = MockServer::start();
    let mut compressed = Vec::new();
    let mut encoder = GzEncoder::new(&mut compressed, Compression::default());
    encoder.write_all(b"name,value\nAna,1\nBia,2\n")?;
    encoder.finish()?;
    server.mock(|when, then| {
        when.method(GET).path("/ft_diarias_2014.csv.gz");
        then.status(200).body(compressed.clone());
    });

    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());
    let resource = CkanResource {
        id: "cfba57bb-358b-4b43-96e6-477920e39f19".to_string(),
        url: format!("{}/ft_diarias_2014.csv.gz", server.url("")),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 2);
    Ok(())
}
