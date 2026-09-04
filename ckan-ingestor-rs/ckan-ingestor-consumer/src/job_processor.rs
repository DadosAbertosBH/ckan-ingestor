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
use ckan_ingestor_lib::datafusion_ckan_data_ingestor::DatafusionCkanDataIngestor;
use ckan_ingestor_lib::datafusion_ducklake_factory::DatafusionDucklakeFactory;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use ckan_ingestor_lib::readers::datastore_reader::DatastoreReader;
use ckan_ingestor_lib::readers::document_reader::DocumentReader;
use ckan_ingestor_lib::readers::json_reader::JsonReader;
use ckan_ingestor_lib::readers::multiple_reader::MultipleReader;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use reqwest::blocking::Client;
use std::{sync::Arc, time::Duration};
use tokio::runtime::Runtime;

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
    factory: DatafusionDucklakeFactory,
    runtime: Arc<Runtime>,
}

impl RealJobProcessor {
    pub fn new(s3: S3DocumentIngestor, factory: DatafusionDucklakeFactory) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            s3,
            factory,
            runtime: Arc::new(runtime),
        })
    }
}

impl Clone for RealJobProcessor {
    fn clone(&self) -> Self {
        Self {
            s3: self.s3.clone(),
            factory: self.factory.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl JobProcessor for RealJobProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        match run_ingestion(&self.runtime, &self.factory, &job, &self.s3) {
            Ok(outcome) => job_result_from_outcome(job.job_id.clone(), outcome),
            Err(e) => {
                let error_str = format!("{}", e);
                let truncated = &error_str[..error_str.len().min(16_000)];
                log::error!("Job {} failed: {}", job.job_id, truncated);
                JobResultMessage {
                    reader: Some(String::new()),
                    job_id: Some(job.job_id.clone()),
                    status: JobStatus::Failed,
                    rows_processed: None,
                    expected_rows: None,
                    encoding: None,
                    csv_strict_mode: None,
                    csv_delimiter: None,
                    expected_columns: None,
                    datastore_active: Some(false),
                    resource_id: None,
                    dataset_name: None,
                    resource_name: None,
                    resource_url: None,
                    resource_format: None,
                    instance_id: None,
                    ckan_url: None,
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
    if outcome.status == ckan_ingestor_lib::ingestor_outcome::IngestionStatus::Failed
        && let Some(error_message) = &outcome.error_message
    {
        log::error!("Job {} failed: {}", job_id, error_message);
    }

    JobResultMessage {
        job_id: Some(job_id),
        reader: Some(outcome.reader),
        status: match outcome.status {
            ckan_ingestor_lib::ingestor_outcome::IngestionStatus::Success => JobStatus::Success,
            ckan_ingestor_lib::ingestor_outcome::IngestionStatus::Failed => JobStatus::Failed,
        },
        rows_processed: i64::try_from(outcome.rows_processed).ok(),
        expected_rows: outcome
            .expected_rows
            .and_then(|value| i64::try_from(value).ok()),
        encoding: outcome.encoding,
        csv_strict_mode: outcome.csv_strict_mode,
        csv_delimiter: outcome.csv_delimiter,
        expected_columns: outcome
            .expected_columns
            .and_then(|value| i64::try_from(value).ok()),
        datastore_active: Some(outcome.datastore_active),
        resource_id: None,
        dataset_name: None,
        resource_name: None,
        resource_url: None,
        resource_format: None,
        instance_id: None,
        ckan_url: None,
        error_message: outcome.error_message,
        preview: Some(outcome.preview),
    }
}

fn run_ingestion(
    runtime: &Runtime,
    factory: &DatafusionDucklakeFactory,
    job: &JobMessage,
    s3: &S3DocumentIngestor,
) -> Result<ckan_ingestor_lib::ingestor_outcome::IngestionOutcome, anyhow::Error> {
    let resource = CkanResource {
        id: job.resource_id.clone(),
        url: job.resource_url.clone(),
        format: job.resource_format.clone(),
        datastore_active: job.datastore_active,
    };
    let http_client = Client::builder()
        .timeout(Duration::from_secs(1200))
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        )
        .build()?;
    let reader = MultipleReader::new(vec![
        Box::new(DatastoreReader::new(
            job.ckan_url.clone(),
            http_client.clone(),
        )),
        Box::new(CsvReader::with_delimiter(
            http_client.clone(),
            job.csv_delimiter.clone(),
        )),
        Box::new(JsonReader::with_client(http_client)),
        Box::new(DocumentReader::new(s3)),
    ]);
    let ingestor = DatafusionCkanDataIngestor::new(factory, &reader);
    Ok(runtime.block_on(ingestor.ingest_ckan_data(&resource)))
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
                csv_strict_mode: Some(false),
                csv_delimiter: Some(";".to_string()),
                datastore_active: true,
                expected_columns: Some(3),
                error_message: None,
                status: IngestionStatus::Success,
            },
        );

        assert_eq!(result.status, JobStatus::Success);
        assert_eq!(result.rows_processed, Some(42));
        assert_eq!(result.expected_rows, Some(50));
        assert_eq!(result.expected_columns, Some(3));
        assert_eq!(result.csv_strict_mode, Some(false));
        assert_eq!(result.csv_delimiter.as_deref(), Some(";"));
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
                csv_strict_mode: None,
                csv_delimiter: None,
                datastore_active: false,
                expected_columns: None,
                error_message: Some("No data to create table from".to_string()),
                status: IngestionStatus::Failed,
            },
        );

        assert_eq!(result.status, JobStatus::Failed);
        assert_eq!(
            result.error_message.as_deref(),
            Some("No data to create table from")
        );
        assert_eq!(serde_json::to_value(&result).unwrap()["status"], "FAILED");
    }
}
