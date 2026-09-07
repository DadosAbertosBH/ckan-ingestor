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
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
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
use crate::parquet_uploader::ParquetUploader;

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
    uploader: ParquetUploader,
    runtime: Arc<Runtime>,
}

impl RealJobProcessor {
    pub fn new(s3: S3DocumentIngestor, uploader: ParquetUploader) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            s3,
            uploader,
            runtime: Arc::new(runtime),
        })
    }
}

impl Clone for RealJobProcessor {
    fn clone(&self) -> Self {
        Self {
            s3: self.s3.clone(),
            uploader: self.uploader.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl JobProcessor for RealJobProcessor {
    fn process(&self, job: JobMessage) -> JobResultMessage {
        match run_conversion(&self.runtime, &self.uploader, &job, &self.s3) {
            Ok(result) => result,
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
                    resource_id: job.resource_id.clone(),
                    dataset_name: None,
                    resource_name: None,
                    resource_url: None,
                    resource_format: None,
                    instance_id: None,
                    ckan_url: None,
                    error_message: Some(truncated.to_string()),
                    preview: None,
                    artifact: None,
                }
            }
        }
    }
}

fn job_result_from_success(
    job_id: String,
    resource_id: String,
    outcome: ckan_ingestor_lib::readers::ckan_reader::SuccessResult,
    artifact: ckan_ingestor_worker_lib::ParquetArtifact,
) -> JobResultMessage {
    JobResultMessage {
        job_id: Some(job_id),
        reader: Some(outcome.reader),
        status: JobStatus::Success,
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
        datastore_active: None,
        resource_id,
        dataset_name: None,
        resource_name: None,
        resource_url: None,
        resource_format: None,
        instance_id: None,
        ckan_url: None,
        error_message: None,
        preview: Some(outcome.preview),
        artifact: Some(artifact),
    }
}

fn run_conversion(
    runtime: &Runtime,
    uploader: &ParquetUploader,
    job: &JobMessage,
    s3: &S3DocumentIngestor,
) -> Result<JobResultMessage, anyhow::Error> {
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
    match reader.read(&resource) {
        Ok(result) => {
            let artifact = runtime.block_on(uploader.upload(
                &job.resource_id,
                &job.job_id,
                &result.parquet,
            ))?;
            Ok(job_result_from_success(
                job.job_id.clone(),
                job.resource_id.clone(),
                result,
                artifact,
            ))
        }
        Err(failed) => Ok(job_result_from_failure(
            &job.job_id,
            &job.resource_id,
            job.datastore_active,
            failed,
        )),
    }
}

fn job_result_from_failure(
    job_id: &str,
    resource_id: &str,
    datastore_active: bool,
    failed: ckan_ingestor_lib::readers::ckan_reader::FailedResult,
) -> JobResultMessage {
    JobResultMessage {
        job_id: Some(job_id.into()),
        status: JobStatus::Failed,
        resource_id: resource_id.into(),
        dataset_name: None,
        resource_name: None,
        resource_url: None,
        resource_format: None,
        instance_id: None,
        ckan_url: None,
        datastore_active: Some(datastore_active),
        reader: Some(failed.reader),
        rows_processed: Some(0),
        expected_rows: failed
            .expected_rows
            .and_then(|value| i64::try_from(value).ok()),
        encoding: None,
        csv_strict_mode: None,
        csv_delimiter: None,
        expected_columns: failed
            .expected_columns
            .and_then(|value| i64::try_from(value).ok()),
        error_message: Some(failed.error.to_string()),
        preview: Some(vec![]),
        artifact: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use ckan_ingestor_lib::{
        parquet_output::ParquetOutput,
        readers::ckan_reader::{FailedResult, SuccessResult},
    };
    use ckan_ingestor_worker_lib::ParquetArtifact;

    use super::{job_result_from_failure, job_result_from_success};
    use crate::messages::JobStatus;

    fn successful_result() -> SuccessResult {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["Ana", "Bia"]))],
        )
        .unwrap();
        let mut parquet = ParquetOutput::try_new(&batch).unwrap();
        parquet.write(&batch).unwrap();
        parquet.finish().unwrap();
        SuccessResult::from_csv(
            parquet,
            "latin-1".into(),
            false,
            ";".into(),
            "test-reader".into(),
        )
    }

    #[test]
    fn maps_successful_conversion_to_the_internal_artifact_contract() {
        let result = job_result_from_success(
            "job-1".into(),
            "resource-1".into(),
            successful_result(),
            ParquetArtifact {
                uri: "s3://warehouse/resource-1/job-1.parquet".into(),
                schema_ipc_base64: "schema".into(),
                num_rows: 2,
                file_size_bytes: 100,
                footer_size_bytes: None,
                column_statistics: vec![],
            },
        );

        assert_eq!(result.status, JobStatus::Success);
        assert_eq!(result.resource_id, "resource-1");
        assert_eq!(result.rows_processed, Some(2));
        assert_eq!(result.csv_delimiter.as_deref(), Some(";"));
        assert_eq!(
            result.artifact.unwrap().uri,
            "s3://warehouse/resource-1/job-1.parquet"
        );
    }

    #[test]
    fn maps_failed_conversion_to_the_protocol_failed_status() {
        let failed =
            FailedResult::from_string("No data to create table from", "test-reader".into());
        let result = job_result_from_failure("job-1", "resource-1", false, failed);

        assert_eq!(result.status, JobStatus::Failed);
        assert_eq!(result.resource_id, "resource-1");
        assert_eq!(
            result.error_message.as_deref(),
            Some("No data to create table from")
        );
        assert!(result.artifact.is_none());
    }
}
