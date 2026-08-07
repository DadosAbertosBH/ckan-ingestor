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
use crate::ckan_reader::CkanReader;
use crate::ckan_resource::CkanResource;
use anyhow::Result;
use duckdb::arrow::array::RecordBatch;
use duckdb::Connection;
use std::cell::RefCell;

/// Mirrors Swift's `CsvReader` — simplified encoding fallback using only
/// DuckDB `read_csv`. Downloads the file first (like Swift), then tries
/// encodings: utf-8 → latin-1 → utf-16.
///
/// DuckDB's `read_csv` auto-detects delimiters, so no explicit delimiter
/// loop is needed.
pub struct CsvReader<'a> {
    conn: &'a Connection,
    /// The last encoding that successfully parsed a CSV.
    last_encoding: RefCell<Option<String>>,
}

impl<'a> CsvReader<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            last_encoding: RefCell::new(None),
        }
    }

    /// Returns the last encoding that successfully parsed a CSV.
    pub fn last_encoding(&self) -> Option<String> {
        self.last_encoding.borrow().clone()
    }

    /// Read CSV batches matching Swift's encoding fallback chain.
    ///
    /// Tries encodings in order:
    ///   1. utf-8 via DuckDB `read_csv`
    ///   2. latin-1 via DuckDB `read_csv`
    ///   3. utf-16 via DuckDB `read_csv`
    ///
    /// Downloads the file first (if remote), matching Swift's approach.
    pub fn read_batches(&self, resource: &CkanResource) -> Result<Vec<RecordBatch>> {
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

        let encodings = ["utf-8", "latin-1", "utf-16"];

        for encoding in &encodings {
            match self.try_read_csv(&csv_path, encoding) {
                Ok(batches) => {
                    *self.last_encoding.borrow_mut() = Some(encoding.to_string());
                    return Ok(batches);
                }
                Err(_) => continue,
            }
        }

        anyhow::bail!("Failed to parse CSV file from {}", resource.url)
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
}

impl CkanReader for CsvReader<'_> {
    fn supported_formats(&self) -> Vec<String> {
        vec!["CSV".to_string()]
    }

    fn do_read(&self, resource: &CkanResource) -> Result<Vec<RecordBatch>> {
        self.read_batches(resource)
    }
}
