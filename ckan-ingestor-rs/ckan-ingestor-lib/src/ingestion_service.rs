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
use crate::duckdb_ckan_data_ingestor::DuckdbCkanDataIngestor;
use crate::ingestion_orchestrator::{compute_column_labels, compute_labels, IngestionOutcome};
use crate::s3_document_ingestor::S3DocumentIngestor;
use anyhow::Result;
use duckdb::arrow::array::RecordBatch;

/// High-level ingestion service — mirrors Python's `_run_ingestion_sync`
/// but lives in the library layer so the worker stays thin.
///
/// The worker (consumer) calls `run` and only deals with Kafka I/O.
pub struct IngestionService;

impl IngestionService {
    /// Run the full ingestion pipeline for a single resource.
    ///
    /// Returns an `IngestionOutcome` ready to be serialized and published
    /// to the `ckan.ingest.jobs_result` topic.
    pub fn run(
        conn: &duckdb::Connection,
        resource_id: &str,
        resource_url: &str,
        resource_format: &str,
        datastore_url: &str,
        s3: &S3DocumentIngestor,
    ) -> Result<IngestionOutcome> {
        Self::run_impl(
            conn,
            resource_id,
            resource_url,
            resource_format,
            datastore_url,
            s3,
        )
    }

    fn run_impl(
        conn: &duckdb::Connection,
        resource_id: &str,
        resource_url: &str,
        resource_format: &str,
        datastore_url: &str,
        s3: &S3DocumentIngestor,
    ) -> Result<IngestionOutcome> {
        // Ensure required tables exist
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ckan_resource_last_update \
             (ckan_resource_id VARCHAR, last_modified TIMESTAMP)",
        )?;

        let resource = CkanResource {
            id: resource_id.to_string(),
            url: resource_url.to_string(),
            format: resource_format.to_string(),
            datastore_active: false,
        };
        let datastore_active = false;
        let expected_rows: Option<i64> = None;
        let expected_columns: Option<i64> = None;

        let csv_reader = CsvReader::new(conn);
        let datastore_reader = DatastoreReader::new(datastore_url.to_string());
        let ingestor = DuckdbCkanDataIngestor::new(conn, s3);

        let ingested =
            ingestor.ingest_ckan_data(&resource, &csv_reader, &datastore_reader, None)?;

        if !ingested {
            let encoding = csv_reader.last_encoding();
            return Ok(IngestionOutcome::new(
                0,
                vec![],
                expected_rows,
                None,
                encoding,
                datastore_active,
                expected_columns,
                vec![],
            ));
        }

        // Get row count and preview
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM \"{}\"", resource_id),
            [],
            |row| row.get(0),
        )?;

        let preview_rows = Self::fetch_preview(conn, resource_id)?;
        let column_count = preview_column_count(&preview_rows);
        let encoding = csv_reader.last_encoding();

        let mut labels = compute_labels(
            count,
            datastore_active,
            &encoding,
            expected_rows,
            None, // resource_size (available from ckan_resource but not yet plumbed)
        );
        compute_column_labels(&mut labels, column_count, expected_columns);

        Ok(IngestionOutcome::new(
            count,
            preview_rows,
            expected_rows,
            None,
            encoding,
            datastore_active,
            expected_columns,
            labels,
        ))
    }

    /// Fetch up to 5 preview rows from the ingested table.
    fn fetch_preview(
        conn: &duckdb::Connection,
        resource_id: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let mut stmt = conn.prepare(&format!("SELECT * FROM \"{}\" LIMIT 5", resource_id))?;
        let arrow_iter = stmt.query_arrow([])?;
        let batches: Vec<RecordBatch> = arrow_iter.collect();

        let mut rows = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut row_map = serde_json::Map::new();
                let schema = batch.schema();
                for col_idx in 0..batch.num_columns() {
                    let name = schema.field(col_idx).name();
                    let val = if batch.column(col_idx).is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        let s = duckdb::arrow::util::display::array_value_to_string(
                            batch.column(col_idx),
                            row_idx,
                        )
                        .unwrap_or_default();
                        serde_json::Value::String(s)
                    };
                    row_map.insert(name.clone(), val);
                }
                rows.push(serde_json::Value::Object(row_map));
            }
        }
        Ok(rows)
    }
}

fn preview_column_count(preview: &[serde_json::Value]) -> usize {
    if preview.is_empty() {
        0
    } else if let Some(serde_json::Value::Object(map)) = preview.first() {
        map.len()
    } else {
        0
    }
}
