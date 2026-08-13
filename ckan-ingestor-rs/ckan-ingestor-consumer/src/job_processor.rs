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
use duckdb::Connection;

use crate::duckdb_factory::DuckdbFactory;
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

pub struct RealJobProcessor {
    s3: S3DocumentIngestor,
    factory: DuckdbFactory,
    conn: Connection,
}

impl RealJobProcessor {
    pub fn new(s3: S3DocumentIngestor, factory: DuckdbFactory) -> anyhow::Result<Self> {
        let conn = factory.open()?;
        Ok(Self { s3, factory, conn })
    }
}

impl Clone for RealJobProcessor {
    fn clone(&self) -> Self {
        // Clone the underlying connection and re-apply session settings, which
        // are per-connection and not inherited by `try_clone`. Each clone runs
        // on its own OS thread (one per Kafka partition), so each gets its own
        // connection to the same DuckLake catalog.
        let conn = self
            .conn
            .try_clone()
            .expect("failed to clone duckdb connection");
        self.factory
            .configure(&conn)
            .expect("failed to configure cloned duckdb connection");
        Self {
            s3: self.s3.clone(),
            factory: self.factory.clone(),
            conn,
        }
    }
}

impl JobProcessor for RealJobProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        let datastore_url = format!("{}/datastore/dump", job.ckan_url.trim_end_matches('/'));

        let result = run_ingestion(&self.conn, &job, &datastore_url, &self.s3);

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
    conn: &Connection,
    job: &JobMessage,
    datastore_url: &str,
    s3: &S3DocumentIngestor,
) -> Result<ckan_ingestor_lib::ingestion_orchestrator::IngestionOutcome, anyhow::Error> {
    IngestionService::run(
        conn,
        &job.resource_id,
        &job.resource_url,
        &job.resource_format,
        datastore_url,
        s3,
    )
    .map_err(|e| anyhow::anyhow!(e))
}
