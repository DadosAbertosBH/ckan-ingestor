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
use ckan_ingestor_lib::ckan_reader::CkanReader;
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::csv_reader::CsvReader;
use common::fixture_path;

#[test]
fn parse_latin_encoded_csv() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn);

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
    let batches = reader.read(&resource)?;
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert!(total > 0, "Should parse at least 1 row");
    assert!(reader.last_encoding().is_some());
    Ok(())
}

#[test]
fn parse_non_latin_and_non_utf8() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn);

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("non_latin1_and_non_utf8.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let batches = reader.read(&resource)?;
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 2);
    // This file uses latin-1 encoding that only works after the PyArrow fallback
    // with semicolon delimiter
    assert!(reader.last_encoding().is_some());
    Ok(())
}

#[test]
fn csv_with_bom() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn);

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };
    let batches = reader.read(&resource)?;
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 804);
    assert!(reader.last_encoding().is_some());
    Ok(())
}
