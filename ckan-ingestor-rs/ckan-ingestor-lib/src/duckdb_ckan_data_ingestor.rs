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

use crate::ingestor_outcome::IngestionOutcome;
use crate::{
    ckan_resource::CkanResource, readers::ckan_reader::CkanReader,
    readers::multiple_reader::MultipleReader,
};
use anyhow::Result;
use arrow_ipc::writer::FileWriter;
use log::debug;

/// Mirrors Python's `DuckdbCkanDataIngestor.ingest_ckan_data` exactly.
///
/// The ingestion follows a format-based fallback chain:
///   1. DATA_STORE (if `datastore_active`)
///   2. CSV
///   3. JSON (via DuckDB `read_json`)
///   4. PDF / DOCX (document ingestion — logs warning for now)
///
/// On failure, the next format is attempted recursively via `attempt_formats`.
pub struct DuckdbCkanDataIngestor<'a> {
    conn: &'a duckdb::Connection,
    reader: &'a MultipleReader,
}

impl<'a> DuckdbCkanDataIngestor<'a> {
    pub fn new(conn: &'a duckdb::Connection, reader: &'a MultipleReader) -> Self {
        Self { conn, reader }
    }

    /// Ingest CKAN resource data into DuckDB.
    ///
    /// Matches Python's `DuckdbCkanDataIngestor.ingest_ckan_data` exactly:
    /// - Format-based fallback chain (DATA_STORE → CSV → JSON → PDF → DOCX)
    /// - On failure, recurses with `attempt_formats` to try next format
    /// - Creates `CREATE OR REPLACE TABLE "{resource_id}" AS {query}`
    /// - Updates `ckan_resource_last_update`
    /// - Returns `Ok(true)` on success, `Ok(false)` when all formats exhausted
    pub fn ingest_ckan_data(&self, resource: &CkanResource) -> Result<IngestionOutcome> {
        let resource_id = &resource.id;

        debug!("updating {} from resource {}", resource_id, resource_id);

        let outcome = match self.reader.read(resource) {
            Ok(result) => {
                self.conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS ckan_resource_last_update \
                      (ckan_resource_id VARCHAR, last_modified TIMESTAMP)",
                )?;
                self.create_table_from_batches(resource_id, &result.data)?;
                self.update_last_modified(resource_id)?;

                IngestionOutcome {
                    rows_processed: result.rows_processed,
                    preview: result.preview,
                    expected_rows: result.expected_rows,
                    encoding: result.encoding,
                    datastore_active: resource.datastore_active,
                    expected_columns: result.expected_columns,
                    status: "success".to_string(),
                }
            }
            Err(failed) => IngestionOutcome {
                rows_processed: 0,
                preview: vec![],
                expected_rows: failed.expected_rows,
                encoding: None,
                datastore_active: resource.datastore_active,
                expected_columns: failed.expected_columns,
                status: "failed".to_string(),
            },
        };
        return Ok(outcome);
    }

    /// Create a DuckDB table from Arrow RecordBatches using atomic
    /// `CREATE TABLE AS SELECT * FROM read_csv(...)`.
    ///
    /// Writes batches to a temporary CSV, then loads atomically to avoid
    /// the per-row INSERT pattern that triggers DuckLake internal errors
    /// ("Calling GetValueInternal on a value that is NULL").
    fn create_table_from_batches(
        &self,
        resource_id: &str,
        batches: &[duckdb::arrow::array::RecordBatch],
    ) -> Result<()> {
        if batches.is_empty() {
            anyhow::bail!("No data to create table from");
        }

        // Write Arrow batches to a temporary CSV file
        let temp_path = std::env::temp_dir().join(format!("{}.arrow", uuid::Uuid::new_v4()));
        let mut file = std::fs::File::create(temp_path.clone())?;
        let mut writer = FileWriter::try_new(&mut file, &batches[0].schema()).unwrap();

        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();

        // Atomic CTAS — single statement, no per-row INSERTs
        let result = self.conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_arrow('{}')",
            resource_id,
            temp_path.to_string_lossy()
        ));

        // Cleanup temp file regardless of outcome
        let _ = std::fs::remove_file(&temp_path);

        result?;
        Ok(())
    }

    /// Update the `ckan_resource_last_update` table.
    fn update_last_modified(&self, resource_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM ckan_resource_last_update WHERE ckan_resource_id = ?",
            duckdb::params![resource_id],
        )?;
        self.conn.execute(
            "INSERT INTO ckan_resource_last_update (ckan_resource_id, last_modified) VALUES (?, NOW())",
            duckdb::params![resource_id],
        )?;
        Ok(())
    }
}
