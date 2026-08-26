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
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::json_reader::JsonReader;
use httpmock::{Method::GET, MockServer};

#[test]
fn reads_multiple_json_objects() -> Result<()> {
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
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;

    std::fs::remove_file(&path)?;
    assert_eq!(result.rows_processed, 2);
    assert_eq!(result.number_of_columns, 2);
    assert_eq!(result.preview[0]["name"], "Ana");
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
        url: format!("{}/download/datapackage.json", server.base_url()),
        format: "JSON".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;

    download.assert();
    assert_eq!(result.rows_processed, 1);
    assert_eq!(result.number_of_columns, 6);
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
        url: path.to_string_lossy().to_string(),
        format: "JSON".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource);
    std::fs::remove_file(&path)?;
    let result = result?;

    assert_eq!(result.rows_processed, 1);
    Ok(())
}
