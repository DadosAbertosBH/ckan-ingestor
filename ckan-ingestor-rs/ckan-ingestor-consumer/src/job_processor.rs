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

use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::duckdb_ckan_data_ingestor::DuckdbCkanDataIngestor;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use ckan_ingestor_lib::readers::datastore_reader::DatastoreReader;
use ckan_ingestor_lib::readers::document_reader::DocumentReader;
use ckan_ingestor_lib::readers::json_reader::JsonReader;
use ckan_ingestor_lib::readers::multiple_reader::MultipleReader;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use duckdb::Connection;
use reqwest::blocking::Client;
use serde_json::Value;

use crate::duckdb_factory::DuckdbFactory;
use crate::messages::{JobMessage, JobResultMessage, JobStatus};

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
            Ok(outcome) => job_result_from_outcome(job.job_id.clone(), outcome),
            Err(e) => {
                let error_str = format!("{}", e);
                let truncated = &error_str[..error_str.len().min(16_000)];
                JobResultMessage {
                    reader: String::new(),
                    job_id: job.job_id.clone(),
                    status: JobStatus::Failed,
                    rows_processed: None,
                    expected_rows: None,
                    encoding: None,
                    expected_columns: None,
                    datastore_active: false,
                    error_message: Some(truncated.to_string()),
                    preview: None,
                }
            }
        }
    }
}

fn job_result_from_outcome(
    job_id: String,
    outcome: ckan_ingestor_lib::ingestor_outcome::IngestionOutcome,
) -> JobResultMessage {
    JobResultMessage {
        job_id,
        reader: outcome.reader,
        status: match outcome.status {
            ckan_ingestor_lib::ingestor_outcome::IngestionStatus::Success => JobStatus::Success,
            ckan_ingestor_lib::ingestor_outcome::IngestionStatus::Failed => JobStatus::Failed,
        },
        rows_processed: i64::try_from(outcome.rows_processed).ok(),
        expected_rows: outcome
            .expected_rows
            .and_then(|value| i64::try_from(value).ok()),
        encoding: outcome.encoding,
        expected_columns: outcome
            .expected_columns
            .and_then(|value| i64::try_from(value).ok()),
        datastore_active: outcome.datastore_active,
        error_message: None,
        preview: Some(outcome.preview),
    }
}

fn run_ingestion(
    conn: &Connection,
    job: &JobMessage,
    datastore_url: &str,
    datastore_active: bool,
    s3: &S3DocumentIngestor,
) -> Result<ckan_ingestor_lib::ingestor_outcome::IngestionOutcome, anyhow::Error> {
    let resource = CkanResource {
        id: job.resource_id.clone(),
        url: job.resource_url.clone(),
        format: job.resource_format.clone(),
        datastore_active,
    };
    let reader = MultipleReader::new(vec![
        Box::new(DatastoreReader::new(datastore_url.to_string())),
        Box::new(CsvReader::new(conn)),
        Box::new(JsonReader::new(conn)),
        Box::new(DocumentReader::new(s3)),
    ]);
    let ingestor = DuckdbCkanDataIngestor::new(conn, &reader);

    ingestor.ingest_ckan_data(&resource)
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

#[cfg(test)]
mod tests {
    use ckan_ingestor_lib::ingestor_outcome::{IngestionOutcome, IngestionStatus};

    use super::job_result_from_outcome;
    use crate::messages::JobStatus;

    #[test]
    fn maps_the_reader_outcome_to_the_python_owned_result_contract() {
        let result = job_result_from_outcome(
            "job-1".to_string(),
            IngestionOutcome {
                reader: "test-reader".to_string(),
                rows_processed: 42,
                preview: vec![serde_json::json!({"name": "Ana"})],
                expected_rows: Some(50),
                encoding: Some("latin-1".to_string()),
                datastore_active: true,
                expected_columns: Some(3),
                status: IngestionStatus::Success,
            },
        );

        assert_eq!(result.status, JobStatus::Success);
        assert_eq!(result.rows_processed, Some(42));
        assert_eq!(result.expected_rows, Some(50));
        assert_eq!(result.expected_columns, Some(3));
        assert_eq!(
            result.preview,
            Some(vec![serde_json::json!({"name": "Ana"})])
        );
    }

    #[test]
    fn maps_failed_outcomes_to_the_protocol_failed_status() {
        let result = job_result_from_outcome(
            "job-1".to_string(),
            IngestionOutcome {
                reader: "test-reader".to_string(),
                rows_processed: 0,
                preview: vec![],
                expected_rows: None,
                encoding: None,
                datastore_active: false,
                expected_columns: None,
                status: IngestionStatus::Failed,
            },
        );

        assert_eq!(result.status, JobStatus::Failed);
        assert_eq!(serde_json::to_value(&result).unwrap()["status"], "FAILED");
    }
}
