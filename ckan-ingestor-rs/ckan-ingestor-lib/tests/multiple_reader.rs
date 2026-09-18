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
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use ckan_ingestor_lib::readers::multiple_reader::{
    FormatResolver, HttpFormatResolver, MultipleReader,
};
use httpmock::Method::{GET, HEAD};
use httpmock::MockServer;
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

fn resource(url: &str, format: &str) -> CkanResource {
    CkanResource {
        id: "resource-id".to_string(),
        package_id: String::new(),
        url: url.to_string(),
        format: format.to_string(),
        datastore_active: false,
        last_modified: String::new(),
    }
}

#[test]
fn resolves_a_missing_format_from_the_content_type_header() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(HEAD).path("/data");
        then.status(200).header("Content-Type", "text/csv");
    });
    let resolver = HttpFormatResolver::new(test_client());

    let format = resolver.resolve(&resource(&format!("{}/data", server.url("")), ""));

    assert_eq!(format.as_deref(), Some("CSV"));
}

#[test]
fn resolves_a_missing_format_from_the_content_disposition_header() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(HEAD).path("/download");
        then.status(200)
            .header("Content-Type", "application/octet-stream")
            .header(
                "Content-Disposition",
                "attachment; filename=\"relatorio.csv\"",
            );
    });
    let resolver = HttpFormatResolver::new(test_client());

    let format = resolver.resolve(&resource(&format!("{}/download", server.url("")), ""));

    assert_eq!(format.as_deref(), Some("CSV"));
}

#[test]
fn infers_the_format_before_selecting_a_reader_end_to_end() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(HEAD).path("/data");
        then.status(200).header("Content-Type", "text/csv");
    });
    server.mock(|when, then| {
        when.method(GET).path("/data");
        then.status(200)
            .header("Content-Type", "text/csv")
            .body("name,value\nAna,1\nBia,2\n");
    });
    let reader = MultipleReader::new(vec![Box::new(CsvReader::new(test_client()))])
        .with_format_resolver(HttpFormatResolver::new(test_client()));

    let result = reader
        .read(&resource(&format!("{}/data", server.url("")), ""))
        .expect("the inferred CSV format should select the CSV reader");

    assert_eq!(result.rows_processed, 2);
}

#[test]
fn reports_an_unsupported_format_when_the_head_request_does_not_help() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(HEAD).path("/missing");
        then.status(404);
    });
    let reader = MultipleReader::new(vec![Box::new(CsvReader::new(test_client()))])
        .with_format_resolver(HttpFormatResolver::new(test_client()));

    let error = match reader.read(&resource(&format!("{}/missing", server.url("")), "")) {
        Ok(_) => panic!("an unresolved empty format must be reported as unsupported"),
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("unsupported format"),
        "unexpected error: {error}"
    );
}
