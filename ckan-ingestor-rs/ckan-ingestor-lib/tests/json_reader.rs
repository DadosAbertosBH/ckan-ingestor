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
use ckan_ingestor_lib::readers::json_reader::JsonReader;
use httpmock::{Method::GET, MockServer};

#[test]
fn rejects_json_lines_documents() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-json-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        "{\"name\":\"Ana\",\"age\":30}\n{\"name\":\"Bia\",\"age\":25}\n",
    )?;

    let reader = JsonReader::new();
    let resource = CkanResource {
        id: "json-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource);

    std::fs::remove_file(&path)?;
    assert!(result.is_err());
    Ok(())
}

#[test]
fn reads_geojson_feature_collection_as_geoparquet() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-geojson-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"{
          "type": "FeatureCollection",
          "features": [
            {"type": "Feature", "properties": {"ID_LT": "1", "NULOTCTM": 10}, "geometry": {"type": "Point", "coordinates": [-43.9, -22.9]}},
            {"type": "Feature", "properties": {"ID_LT": "2", "NULOTCTM": 20}, "geometry": {"type": "Polygon", "coordinates": [[[-43.9, -22.9], [-43.8, -22.9], [-43.9, -22.9]]]}}
          ]
        }"#,
    )?;
    let resource = CkanResource {
        id: "geojson-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = JsonReader::new().read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;
    assert_eq!(result.rows_processed, 2);
    assert!(result.parquet.schema.field_with_name("geometry").is_ok());
    assert!(result.parquet.schema.field_with_name("ID_LT").is_ok());
    assert!(result.parquet.schema.field_with_name("NULOTCTM").is_ok());
    assert!(result
        .parquet
        .metadata()
        .and_then(|metadata| metadata.file_metadata().key_value_metadata())
        .is_some_and(|metadata| metadata.iter().any(|entry| entry.key == "geo")));
    Ok(())
}

#[test]
fn reads_regular_json_array() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-json-array-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"[{"name":"Ana","age":30},{"name":"Bia","age":25}]"#,
    )?;

    let reader = JsonReader::new();
    let resource = CkanResource {
        id: "json-array-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;

    assert_eq!(result.rows_processed, 2);
    assert_eq!(result.number_of_columns, 2);
    assert_eq!(result.preview[1]["name"], "Bia");
    Ok(())
}

#[test]
fn represents_all_null_json_columns_as_utf8() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-null-json-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(&path, r#"[{"name":null}]"#)?;
    let resource = CkanResource {
        id: "null-json-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = JsonReader::new().read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;

    assert_eq!(
        result.parquet.schema.field_with_name("name")?.data_type(),
        &DataType::Utf8
    );
    Ok(())
}

#[test]
fn represents_nested_null_json_fields_as_utf8() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-nested-null-json-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(&path, r#"[{"metadata":{"source":null}}]"#)?;
    let resource = CkanResource {
        id: "nested-null-json-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = JsonReader::new().read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;

    let DataType::Struct(fields) = result
        .parquet
        .schema
        .field_with_name("metadata")?
        .data_type()
    else {
        panic!("metadata should be a struct");
    };
    assert_eq!(
        fields
            .iter()
            .find(|field| field.name() == "source")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    Ok(())
}

#[test]
fn reads_remote_datapackage_json() -> Result<()> {
    let server = MockServer::start();
    let body = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/datapackage.json"
    ))?;
    let download = server.mock(|when, then| {
        when.method(GET).path("/download/datapackage.json");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body.clone());
    });

    let reader = JsonReader::new();
    let resource = CkanResource {
        id: "7b77f87c-f850-44e8-96d2-5bdcdbd88dd8".to_string(),
        package_id: String::new(),
        url: format!("{}/download/datapackage.json", server.base_url()),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;

    download.assert();
    assert_eq!(result.rows_processed, 1);
    assert_eq!(result.number_of_columns, 6);
    Ok(())
}

#[test]
fn coerces_mixed_type_fields_to_string() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-mixed-types-json-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"[
          {"Id": 106097, "DataAssinatura": "01/11/2022:07:03"},
          {"Id": "35d5bc0d-b053-4124-b314-e10c287cf1fa", "DataAssinatura": "30/11/2022:23:44"}
        ]"#,
    )?;
    let resource = CkanResource {
        id: "mixed-types-json-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = JsonReader::new().read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;

    assert_eq!(result.rows_processed, 2);
    assert_eq!(
        result.parquet.schema.field_with_name("Id")?.data_type(),
        &DataType::Utf8
    );
    assert_eq!(result.preview[0]["Id"], "106097");
    assert_eq!(
        result.preview[1]["Id"],
        "35d5bc0d-b053-4124-b314-e10c287cf1fa"
    );
    Ok(())
}

#[test]
fn reads_json_object_larger_than_default_maximum_object_size() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-large-json-reader-{}.json",
        std::process::id()
    ));
    let json = format!(r#"{{"payload":"{}"}}"#, "x".repeat(34 * 1024 * 1024));
    std::fs::write(&path, json)?;

    let reader = JsonReader::new();
    let resource = CkanResource {
        id: "large-json-resource".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;

    assert_eq!(result.rows_processed, 1);
    Ok(())
}
