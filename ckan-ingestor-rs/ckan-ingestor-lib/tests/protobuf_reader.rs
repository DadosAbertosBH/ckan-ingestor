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

use anyhow::Result;
use arrow::datatypes::DataType;
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::multiple_reader::{HttpFormatResolver, MultipleReader};
use ckan_ingestor_lib::readers::protobuf_reader::{ProtobufReader, PROTOBUF_FORMAT};
use httpmock::Method::{GET, HEAD};
use httpmock::MockServer;
use std::time::Duration;

const GTFS_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/data/gtfs.binpb"
);

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
        id: "gtfs-resource".to_string(),
        package_id: String::new(),
        url: url.to_string(),
        format: format.to_string(),
        datastore_active: false,
        last_modified: String::new(),
    }
}

#[test]
fn exposes_the_protobuf_format() {
    let reader = ProtobufReader::new();

    assert_eq!(reader.supported_formats(), &[PROTOBUF_FORMAT.to_string()]);
    assert!(reader.can_read(&resource("gtfs.binpb", "protobuf")));
    assert!(reader.can_read(&resource("gtfs.binpb", "PROTOBUF")));
    assert!(!reader.can_read(&resource("gtfs.binpb", "CSV")));
}

#[test]
fn reads_gtfs_realtime_feed_as_one_row_per_entity() -> Result<()> {
    let reader = ProtobufReader::new();

    let result = reader.read(&resource(GTFS_FIXTURE, PROTOBUF_FORMAT))?;

    assert_eq!(result.rows_processed, 1290);
    assert!(result.parquet.schema.field_with_name("id").is_ok());
    assert!(result.parquet.schema.field_with_name("trip_update").is_ok());
    assert_eq!(
        result.preview[0]["id"],
        serde_json::json!("21303 - 2661974S307679P141000")
    );
    Ok(())
}

#[test]
fn exposes_nested_gtfs_structures() -> Result<()> {
    let reader = ProtobufReader::new();

    let result = reader.read(&resource(GTFS_FIXTURE, PROTOBUF_FORMAT))?;

    let DataType::Struct(trip_update_fields) = result
        .parquet
        .schema
        .field_with_name("trip_update")?
        .data_type()
    else {
        panic!("trip_update should be a struct");
    };
    assert!(trip_update_fields
        .iter()
        .any(|field| field.name() == "trip"));
    assert!(trip_update_fields
        .iter()
        .any(|field| field.name() == "stop_time_update"));

    let stop_time_update = trip_update_fields
        .iter()
        .find(|field| field.name() == "stop_time_update")
        .expect("stop_time_update should be present");
    let DataType::List(item) = stop_time_update.data_type() else {
        panic!("stop_time_update should be a list");
    };
    assert!(matches!(item.data_type(), DataType::Struct(_)));
    Ok(())
}

#[test]
fn uses_the_proto_field_names() -> Result<()> {
    let reader = ProtobufReader::new();

    let result = reader.read(&resource(GTFS_FIXTURE, PROTOBUF_FORMAT))?;

    let DataType::Struct(trip_update_fields) = result
        .parquet
        .schema
        .field_with_name("trip_update")?
        .data_type()
    else {
        panic!("trip_update should be a struct");
    };
    let DataType::Struct(trip_fields) = trip_update_fields
        .iter()
        .find(|field| field.name() == "trip")
        .expect("trip should be present")
        .data_type()
    else {
        panic!("trip should be a struct");
    };
    assert!(trip_fields.iter().any(|field| field.name() == "trip_id"));
    assert!(trip_fields
        .iter()
        .any(|field| field.name() == "schedule_relationship"));
    Ok(())
}

#[test]
fn rejects_bytes_that_are_not_valid_protobuf() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-invalid-protobuf-{}.binpb",
        std::process::id()
    ));
    std::fs::write(&path, b"\x0a")?;

    let result = ProtobufReader::new().read(&resource(&path.to_string_lossy(), PROTOBUF_FORMAT));

    std::fs::remove_file(&path)?;
    assert!(result.is_err());
    Ok(())
}

#[test]
fn reads_remote_gtfs_realtime_feed_inferred_from_the_content_type() -> Result<()> {
    let body = std::fs::read(GTFS_FIXTURE)?;
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(HEAD).path("/gtfs-realtime");
        then.status(200)
            .header("Content-Type", "application/x-google-protobuf");
    });
    server.mock(|when, then| {
        when.method(GET).path("/gtfs-realtime");
        then.status(200)
            .header("Content-Type", "application/x-google-protobuf")
            .body(body);
    });
    let reader = MultipleReader::new(vec![Box::new(ProtobufReader::new())])
        .with_format_resolver(HttpFormatResolver::new(test_client()));

    let result = reader
        .read(&resource(&format!("{}/gtfs-realtime", server.url("")), ""))
        .expect("the inferred PROTOBUF format should select the protobuf reader");

    assert_eq!(result.rows_processed, 1290);
    Ok(())
}
