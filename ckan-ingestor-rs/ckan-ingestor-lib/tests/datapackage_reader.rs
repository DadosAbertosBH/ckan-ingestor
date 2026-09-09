// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;
use arrow::datatypes::DataType;
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::datapackage_reader::DatapackageReader;
use httpmock::{Method::GET, MockServer};
use tempfile::tempdir;

#[test]
fn datapackage_resources_are_represented_as_individual_rows() -> Result<()> {
    let tempdir = tempdir()?;
    let path = tempdir.path().join("datapackage.json");
    std::fs::write(
        &path,
        r#"{
            "profile": "tabular-data-package",
            "resources": [
                {"name": "first", "path": "first.csv"},
                {"name": "second", "path": ["second.csv", "third.csv"]}
            ]
        }"#,
    )?;
    let resource = CkanResource {
        id: "datapackage-resource".to_string(),
        package_id: "package-id".to_string(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let reader = DatapackageReader::new();
    assert!(reader.can_read(&resource));
    let result = reader.read(&resource);
    let result = result?;

    assert_eq!(result.rows_processed, 2);
    assert_eq!(result.number_of_columns, 17);
    assert_eq!(result.preview[0]["package_id"], "package-id");
    assert_eq!(result.preview[0]["name"], "first");
    assert_eq!(result.preview[1]["path"], "[\"second.csv\",\"third.csv\"]");
    Ok(())
}

#[test]
fn static_schema_projects_the_official_resource_contract() {
    let schema = DatapackageReader::schema();
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "package_id",
            "profile",
            "name",
            "path",
            "data",
            "schema",
            "title",
            "description",
            "homepage",
            "dialect",
            "format",
            "mediatype",
            "encoding",
            "hash",
            "sources",
            "licenses",
            "bytes"
        ]
    );
    assert_eq!(
        schema.field_with_name("path").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema.field_with_name("bytes").unwrap().data_type(),
        &DataType::Int64
    );
}

#[test]
fn deserializes_a_remote_datapackage_with_mock() -> Result<()> {
    let server = MockServer::start();
    let body = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/datapackage.json"
    ))?;
    let download = server.mock(|when, then| {
        when.method(GET)
            .path("/dataset/c96021c7-95d7-4f9c-b530-ebb3f1f505f9/resource/b898cbd8-79f6-4c00-a4ff-052988c27bb2/download/datapackage.json");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body.clone());
    });
    let resource = CkanResource {
        id: "b898cbd8-79f6-4c00-a4ff-052988c27bb2".to_string(),
        package_id: "c96021c7-95d7-4f9c-b530-ebb3f1f505f9".to_string(),
        url: format!(
            "{}/dataset/c96021c7-95d7-4f9c-b530-ebb3f1f505f9/resource/b898cbd8-79f6-4c00-a4ff-052988c27bb2/download/datapackage.json",
            server.base_url()
        ),
        format: "JSON".to_string(),
        datastore_active: false,
    last_modified: String::new(),
    };

    let result = DatapackageReader::new().read(&resource)?;

    download.assert();
    assert_eq!(result.rows_processed, 1);
    assert_eq!(result.preview[0]["package_id"], resource.package_id);
    Ok(())
}
