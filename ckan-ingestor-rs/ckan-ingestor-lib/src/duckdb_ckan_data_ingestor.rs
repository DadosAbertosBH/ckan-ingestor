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
use crate::csv_reader::CsvReader;
use crate::datastore_reader::DatastoreReader;
use crate::s3_document_ingestor::S3DocumentIngestor;
use anyhow::Result;
use log::{debug, error, info, warn};

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
    s3: &'a S3DocumentIngestor,
}

impl<'a> DuckdbCkanDataIngestor<'a> {
    pub fn new(conn: &'a duckdb::Connection, s3: &'a S3DocumentIngestor) -> Self {
        Self { conn, s3 }
    }

    /// Ingest CKAN resource data into DuckDB.
    ///
    /// Matches Python's `DuckdbCkanDataIngestor.ingest_ckan_data` exactly:
    /// - Format-based fallback chain (DATA_STORE → CSV → JSON → PDF → DOCX)
    /// - On failure, recurses with `attempt_formats` to try next format
    /// - Creates `CREATE OR REPLACE TABLE "{resource_id}" AS {query}`
    /// - Updates `ckan_resource_last_update`
    /// - Returns `Ok(true)` on success, `Ok(false)` when all formats exhausted
    pub fn ingest_ckan_data(
        &self,
        resource: &CkanResource,
        csv_reader: &CsvReader,
        datastore_reader: &DatastoreReader,
        attempt_formats: Option<Vec<String>>,
    ) -> Result<bool> {
        let mut attempted = attempt_formats.unwrap_or_default();
        self.ingest_ckan_data_inner(resource, csv_reader, datastore_reader, &mut attempted)
    }

    fn ingest_ckan_data_inner(
        &self,
        resource: &CkanResource,
        csv_reader: &CsvReader,
        datastore_reader: &DatastoreReader,
        attempted: &mut Vec<String>,
    ) -> Result<bool> {
        let resource_id = &resource.id;

        debug!("updating {} from resource {}", resource_id, resource_id);

        // 1. Try DATA_STORE if datastore_active and not attempted yet
        if resource.datastore_active && !attempted.contains(&"DATA_STORE".to_string()) {
            attempted.push("DATA_STORE".to_string());

            match self.try_datastore(resource, datastore_reader) {
                DatastoreResult::Success => {
                    self.update_last_modified(resource_id)?;
                    info!("Finished working on {}", resource_id);
                    return Ok(true);
                }
                DatastoreResult::Empty => {
                    info!(
                        "Resource {} datastore is empty, falling back to file-based formats",
                        resource_id
                    );
                    return self.ingest_ckan_data_inner(
                        resource,
                        csv_reader,
                        datastore_reader,
                        attempted,
                    );
                }
                DatastoreResult::Error(e) => {
                    warn!(
                        "Failed to parse {} from resource {} error = {} using formats {:?}",
                        resource_id, resource_id, e, attempted,
                    );
                    return self.ingest_ckan_data_inner(
                        resource,
                        csv_reader,
                        datastore_reader,
                        attempted,
                    );
                }
            }
        }

        // 2. Try CSV
        if resource.format == "CSV" && !attempted.contains(&"CSV".to_string()) {
            attempted.push("CSV".to_string());

            match self.try_csv(csv_reader, resource) {
                Ok(true) => {
                    self.update_last_modified(resource_id)?;
                    info!("Finished working on {}", resource_id);
                    return Ok(true);
                }
                Ok(false) => {
                    // CSV reader failed all encodings, fall through
                    warn!(
                        "Failed to parse {} from resource {} error = CSV parse failed using formats {:?}",
                        resource_id, resource_id, attempted,
                    );
                    return self.ingest_ckan_data_inner(
                        resource,
                        csv_reader,
                        datastore_reader,
                        attempted,
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to parse {} from resource {} error = {} using formats {:?}",
                        resource_id, resource_id, e, attempted,
                    );
                    return self.ingest_ckan_data_inner(
                        resource,
                        csv_reader,
                        datastore_reader,
                        attempted,
                    );
                }
            }
        }

        // 3. Try JSON — DuckDB's read_json handles JSON arrays/objects directly
        if resource.format == "JSON" && !attempted.contains(&"JSON".to_string()) {
            attempted.push("JSON".to_string());

            match self.try_json(resource) {
                Ok(()) => {
                    self.update_last_modified(resource_id)?;
                    info!("Finished working on {}", resource_id);
                    return Ok(true);
                }
                Err(e) => {
                    warn!(
                        "Failed to parse {} from resource {} error = {} using formats {:?}",
                        resource_id, resource_id, e, attempted,
                    );
                    return self.ingest_ckan_data_inner(
                        resource,
                        csv_reader,
                        datastore_reader,
                        attempted,
                    );
                }
            }
        }

        // 4. Try PDF
        if resource.format == "PDF" && !attempted.contains(&"PDF".to_string()) {
            attempted.push("PDF".to_string());

            {
                match self.s3.ingest_blocking(
                    &format!("{}.pdf", resource_id),
                    &resource.url,
                    "application/pdf",
                ) {
                    Ok(download_url) => {
                        self.conn.execute(
                            &format!(
                                "CREATE OR REPLACE TABLE \"{}\" AS SELECT '{}' as url",
                                resource_id, download_url
                            ),
                            [],
                        )?;
                        self.update_last_modified(resource_id)?;
                        info!("Finished working on {}", resource_id);
                        return Ok(true);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to parse {} from resource {} error = {} using formats {:?}",
                            resource_id, resource_id, e, attempted,
                        );
                        return self.ingest_ckan_data_inner(
                            resource,
                            csv_reader,
                            datastore_reader,
                            attempted,
                        );
                    }
                }
            }
        }

        // 5. Try DOCX
        if resource.format == "DOCX" && !attempted.contains(&"DOCX".to_string()) {
            attempted.push("DOCX".to_string());

            {
                match self.s3.ingest_blocking(
                    &format!("{}.docx", resource_id),
                    &resource.url,
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ) {
                    Ok(download_url) => {
                        self.conn.execute(
                            &format!(
                                "CREATE OR REPLACE TABLE \"{}\" AS SELECT '{}' as url",
                                resource_id, download_url
                            ),
                            [],
                        )?;
                        self.update_last_modified(resource_id)?;
                        info!("Finished working on {}", resource_id);
                        return Ok(true);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to parse {} from resource {} error = {} using formats {:?}",
                            resource_id, resource_id, e, attempted,
                        );
                        return self.ingest_ckan_data_inner(
                            resource,
                            csv_reader,
                            datastore_reader,
                            attempted,
                        );
                    }
                }
            }
        }

        // No more formats to try
        error!(
            "Resource {} from resource {} have an unsupported format {}",
            resource_id, resource_id, resource.format
        );
        Ok(false)
    }

    /// Try datastore ingestion. Returns Success, Empty, or Error.
    fn try_datastore(
        &self,
        resource: &CkanResource,
        datastore_reader: &DatastoreReader,
    ) -> DatastoreResult {
        let resource_id = &resource.id;

        match datastore_reader.read_batches(resource) {
            Ok(batches) => {
                if batches.is_empty() {
                    return DatastoreResult::Empty;
                }
                self.create_table_from_batches(resource_id, &batches)
                    .map(|_| DatastoreResult::Success)
                    .unwrap_or_else(DatastoreResult::Error)
            }
            Err(e) => DatastoreResult::Error(e),
        }
    }

    /// Try CSV ingestion with encoding fallback.
    /// Uses atomic `CREATE TABLE AS SELECT * FROM read_csv(...)` to avoid
    /// per-row INSERT pattern that triggers DuckLake internal errors.
    /// Returns Ok(true) on success, Ok(false) if all encodings exhausted,
    /// Err on unexpected errors.
    fn try_csv(&self, csv_reader: &CsvReader, resource: &CkanResource) -> Result<bool> {
        csv_reader.try_create_table(&resource.id, resource)
    }

    /// Try JSON ingestion via DuckDB's read_json.
    fn try_json(&self, resource: &CkanResource) -> Result<()> {
        let resource_id = &resource.id;
        let query = format!(
            "SELECT * FROM read_json('{}', maximum_object_size=4194304)",
            resource.url
        );
        self.conn.execute(
            &format!("CREATE OR REPLACE TABLE \"{}\" AS {}", resource_id, query),
            [],
        )?;
        Ok(())
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
        let temp_path = std::env::temp_dir().join(format!("{}.csv", uuid::Uuid::new_v4()));
        self.write_batches_to_csv(&temp_path, batches)?;

        // Atomic CTAS — single statement, no per-row INSERTs
        let result = self.conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv('{}', header=true, auto_detect=true)",
            resource_id,
            temp_path.to_string_lossy()
        ));

        // Cleanup temp file regardless of outcome
        let _ = std::fs::remove_file(&temp_path);

        result?;
        Ok(())
    }

    /// Write Arrow RecordBatches to a CSV file with proper quoting.
    fn write_batches_to_csv(
        &self,
        path: &std::path::Path,
        batches: &[duckdb::arrow::array::RecordBatch],
    ) -> Result<()> {
        use std::io::Write;

        let mut file = std::fs::File::create(path)?;

        // Write header
        if let Some(first) = batches.first() {
            let schema = first.schema();
            let headers: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| csv_quote_field(f.name()))
                .collect();
            writeln!(file, "{}", headers.join(","))?;
        }

        // Write data rows
        for batch in batches {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();

            for row_idx in 0..num_rows {
                let mut row_parts: Vec<String> = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
                    let col = batch.column(col_idx);
                    if col.is_null(row_idx) {
                        row_parts.push(String::new());
                    } else {
                        let val =
                            duckdb::arrow::util::display::array_value_to_string(col, row_idx)?;
                        row_parts.push(csv_quote_field(&val));
                    }
                }
                writeln!(file, "{}", row_parts.join(","))?;
            }
        }

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

/// Quote a CSV field if it contains special characters (commas, quotes, newlines).
fn csv_quote_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        let escaped = field.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        field.to_string()
    }
}

enum DatastoreResult {
    Success,
    Empty,
    Error(anyhow::Error),
}
