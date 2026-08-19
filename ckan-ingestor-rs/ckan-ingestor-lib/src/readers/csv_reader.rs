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
use crate::ckan_resource::CkanResource;
use crate::readers::ckan_reader::{CkanReader, FailedResult, ReadResult, SuccessResult};
use anyhow::Result;
use duckdb::arrow::array::RecordBatch;
use duckdb::Connection;

pub struct CsvReader<'a> {
    conn: &'a Connection,
    supported_formats: Vec<String>,
}

impl<'a> CsvReader<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            supported_formats: vec!["CSV".to_string()],
        }
    }

    /// Read CSV batches matching Swift's encoding fallback chain.
    ///
    /// Tries encodings in order:
    ///   1. utf-8 via DuckDB `read_csv`
    ///   2. latin-1 via DuckDB `read_csv`
    ///   3. utf-16 via DuckDB `read_csv`
    ///
    /// Downloads the file first (if remote), matching Swift's approach.
    pub fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let csv_path = if is_remote {
            self.download_to_temp(&resource.url)?
        } else {
            resource.url.clone()
        };

        // Cleanup temp file when done
        struct Cleanup(Option<String>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                if let Some(ref path) = self.0 {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        let _cleanup = Cleanup(if is_remote {
            Some(csv_path.clone())
        } else {
            None
        });

        let encodings = ["utf-8", "latin-1", "CP1252"];

        for encoding in encodings {
            match self.try_read_csv(&csv_path, encoding) {
                Ok(batches) => return Ok(SuccessResult::from_csv(batches, encoding.to_string())),
                Err(_) => continue,
            }
        }

        let error = format!("Failed to parse CSV file from {}", resource.url);
        Err(FailedResult::from_string(&error))
    }

    /// Download a remote file to a temporary location.
    fn download_to_temp(&self, url: &str) -> Result<String> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
            )
            .build()?;
        let response = client.get(url).send()?;
        let bytes = response.bytes()?;
        let temp_path = std::env::temp_dir().join(format!("{}.csv", uuid::Uuid::new_v4()));
        std::fs::write(&temp_path, &bytes)?;
        Ok(temp_path.to_string_lossy().to_string())
    }

    /// Try DuckDB's `read_csv` with a specific encoding.
    fn try_read_csv(&self, path: &str, encoding: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT * FROM read_csv('{}', sample_size=300000, encoding='{}')",
            path, encoding
        ))?;
        let arrow_iter = stmt.query_arrow([])?;
        let batches: Vec<RecordBatch> = arrow_iter.collect();
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            anyhow::bail!("No data");
        }
        Ok(batches)
    }

    /// Create a DuckDB table atomically via `CREATE TABLE AS SELECT * FROM read_csv(...)`.
    ///
    /// Downloads the file if remote, then tries encodings (utf-8, latin-1, utf-16).
    /// Uses atomic CTAS to avoid the per-row INSERT pattern that triggers DuckLake
    /// "Calling GetValueInternal on a value that is NULL" internal errors.
    /// Returns `Ok(true)` on success, `Ok(false)` when all encodings exhausted.
    pub fn try_create_table(&self, resource_id: &str, resource: &CkanResource) -> Result<bool> {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let csv_path = if is_remote {
            self.download_to_temp(&resource.url)?
        } else {
            resource.url.clone()
        };

        // Cleanup temp file when done
        struct Cleanup(Option<String>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                if let Some(ref path) = self.0 {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        let _cleanup = Cleanup(if is_remote {
            Some(csv_path.clone())
        } else {
            None
        });

        let encodings = ["utf-8", "latin-1", "CP1252"];

        for encoding in &encodings {
            match self.try_create_table_with_encoding(resource_id, &csv_path, encoding) {
                Ok(()) => {
                    return Ok(true);
                }
                Err(_) => continue,
            }
        }

        Ok(false)
    }

    /// Execute a single `CREATE OR REPLACE TABLE AS SELECT * FROM read_csv(...)`.
    fn try_create_table_with_encoding(
        &self,
        resource_id: &str,
        path: &str,
        encoding: &str,
    ) -> Result<()> {
        let sql = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv('{}', sample_size=300000, encoding='{}')",
            resource_id, path, encoding
        );
        self.conn.execute_batch(&sql)?;
        Ok(())
    }
}

impl CkanReader for CsvReader<'_> {
    fn supported_formats(&self) -> &[String] {
        return &self.supported_formats;
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("data")
            .join(file)
    }

    /// A semicolon-delimited CSV containing quoted fields with embedded
    /// semicolons must still be parsed into a table with all records.
    #[test]
    fn do_read_parses_windows1152_enconde_semicolon_delimited_csv() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let reader = CsvReader::new(&conn);

        let csv_path = fixture_path("renuncia-fiscal-informacoes-conceituais-2024.csv");
        let resource = CkanResource {
            id: "5d16743c-0f6b-411d-aa7c-734a24b02812".to_string(),
            url: csv_path.to_str().unwrap().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let result = reader.do_read(&resource)?;
        let total: usize = result.data.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total, 31, "should read all 31 data records");

        Ok(())
    }
}
