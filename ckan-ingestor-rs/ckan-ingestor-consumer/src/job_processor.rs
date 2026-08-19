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
use reqwest::blocking::Client;
use serde_json::Value;

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

        // Determine whether the CKAN resource has an active DataStore, so the
        // ingestion can prefer the datastore endpoint over the raw file.
        let datastore_active = fetch_datastore_active(&job);

        let result = run_ingestion(&self.conn, &job, &datastore_url, datastore_active, &self.s3);

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
    datastore_active: bool,
    s3: &S3DocumentIngestor,
) -> Result<ckan_ingestor_lib::ingestor_outcome::IngestionOutcome, anyhow::Error> {
    IngestionService::run(
        conn,
        &job.resource_id,
        &job.resource_url,
        &job.resource_format,
        datastore_url,
        datastore_active,
        s3,
    )
    .map_err(|e| anyhow::anyhow!(e))
}

/// Query CKAN's `resource_show` action to learn whether the resource has an
/// active DataStore (the `datastore_active` flag).
///
/// Returns `false` on any error — the datastore is an optimization, not a hard
/// requirement, so failures must fall back to file-based ingestion.
fn fetch_datastore_active(job: &JobMessage) -> bool {
    if job.ckan_url.is_empty() || job.resource_id.is_empty() {
        return false;
    }

    let url = format!(
        "{}/api/action/resource_show?id={}",
        job.ckan_url.trim_end_matches('/'),
        job.resource_id
    );

    let client = match Client::builder()
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        )
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    let resp = match client.get(&url).send() {
        Ok(r) => r,
        Err(_) => return false,
    };

    let json: Value = match resp.json() {
        Ok(j) => j,
        Err(_) => return false,
    };

    json.get("result")
        .and_then(|r| r.get("datastore_active"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}
