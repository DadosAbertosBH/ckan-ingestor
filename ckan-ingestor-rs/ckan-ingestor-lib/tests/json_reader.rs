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

#[test]
fn reads_a_json_array_of_objects() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "ckan-ingestor-json-reader-{}.json",
        std::process::id()
    ));
    std::fs::write(
        &path,
        r#"[{"name":"Ana","age":30},{"name":"Bia","age":25}]"#,
    )?;

    let conn = duckdb::Connection::open_in_memory()?;
    let reader = JsonReader::new(&conn);
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
