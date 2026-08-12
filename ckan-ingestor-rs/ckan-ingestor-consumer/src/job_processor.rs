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

use ckan_ingestor_lib::ingestion_service::IngestionService;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;

use crate::messages::{JobMessage, JobResultMessage};

// ---------------------------------------------------------------------------
// JobProcessor trait
// ---------------------------------------------------------------------------

/// Processes a `JobMessage` and produces a final `JobResultMessage`.
///
/// Runs synchronously on the worker's dedicated OS thread — the heavy
/// blocking work (duckdb) does not need `spawn_blocking` here because
/// each partition owns its own thread.
pub trait JobProcessor: Clone + Send + 'static {
    fn process(&self, job: JobMessage) -> JobResultMessage;
}

// ---------------------------------------------------------------------------
// RealJobProcessor — production implementation
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct RealJobProcessor {
    s3: S3DocumentIngestor,
}

impl RealJobProcessor {
    pub fn new(s3: S3DocumentIngestor) -> Self {
        Self { s3 }
    }
}

impl JobProcessor for RealJobProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        let datastore_url = format!("{}/datastore/dump", job.ckan_url.trim_end_matches('/'));

        let result = run_ingestion(&job, &datastore_url, &self.s3);

        match result {
            Ok(outcome) => JobResultMessage {
                job_id: job.job_id.clone(),
                status: outcome.status.clone(),
                rows_processed: Some(outcome.rows_processed),
                expected_rows: outcome.expected_rows,
                resource_size: outcome.resource_size,
                encoding: outcome.encoding,
                expected_columns: outcome.expected_columns,
                datastore_active: outcome.datastore_active,
                labels: outcome.labels,
                error_message: None,
                preview: Some(outcome.preview),
            },
            Err(e) => {
                let error_str = format!("{}", e);
                let truncated = &error_str[..error_str.len().min(16_000)];
                JobResultMessage {
                    job_id: job.job_id.clone(),
                    status: "FAILED".to_string(),
                    rows_processed: None,
                    expected_rows: None,
                    resource_size: None,
                    encoding: None,
                    expected_columns: None,
                    datastore_active: false,
                    labels: vec![],
                    error_message: Some(truncated.to_string()),
                    preview: None,
                }
            }
        }
    }
}

fn run_ingestion(
    job: &JobMessage,
    datastore_url: &str,
    s3: &S3DocumentIngestor,
) -> Result<ckan_ingestor_lib::ingestion_orchestrator::IngestionOutcome, anyhow::Error> {
    let db_path = std::env::var("DUCKLAKE_DATABASE").expect("DUCKLAKE_DATABASE must be set");
    let catalog_uri =
        std::env::var("DUCKLAKE_CATALOG_URI").expect("DUCKLAKE_CATALOG_URI must be set");
    let s3_endpoint = std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT must be set");
    let s3_bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET must be set");
    let s3_access_key = std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set");
    let s3_secret_key =
        std::env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set");
    let s3_use_ssl = std::env::var("S3_USE_SSL")
        .map(|v| v == "true")
        .unwrap_or(false);

    let conn = duckdb::Connection::open(&db_path)?;
    conn.execute_batch(&format!("SET s3_endpoint='{}';", s3_endpoint))?;
    conn.execute_batch(&format!("SET s3_use_ssl={};", s3_use_ssl))?;
    conn.execute_batch(&format!("SET s3_access_key_id='{}';", s3_access_key))?;
    conn.execute_batch(&format!("SET s3_secret_access_key='{}';", s3_secret_key))?;
    conn.execute_batch("SET s3_url_style='path';")?;
    conn.execute_batch("SET pg_debug_show_queries=false;")?;
    conn.execute_batch(&format!(
        "ATTACH IF NOT EXISTS 'ducklake:{}' AS lake (DATA_PATH 's3://{}', DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE);",
        catalog_uri, s3_bucket
    ))?;
    conn.execute_batch("USE lake;")?;
    conn.execute_batch("SET ducklake_max_retry_count = 100;")?;
    IngestionService::run(
        &conn,
        &job.resource_id,
        &job.resource_url,
        &job.resource_format,
        datastore_url,
        s3,
    )
    .map_err(|e| anyhow::anyhow!(e))
}
