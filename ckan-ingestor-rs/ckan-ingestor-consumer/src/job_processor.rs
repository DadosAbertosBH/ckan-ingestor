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

use async_stream::stream;
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use ckan_ingestor_lib::readers::datapackage_reader::DatapackageReader;
use ckan_ingestor_lib::readers::datastore_reader::DatastoreReader;
use ckan_ingestor_lib::readers::file_reference_reader::FileReferenceReader;
use ckan_ingestor_lib::readers::json_reader::JsonReader;
use ckan_ingestor_lib::readers::multiple_reader::MultipleReader;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use futures::Stream;
use reqwest::blocking::Client;
use std::{sync::Arc, time::Duration};
use tokio::runtime::Runtime;

use crate::messages::{JobMessage, JobResultMessage, JobStatus};
use crate::parquet_uploader::ParquetUploader;
use message_processor::{MessageProcessor, OutgoingMessage};

// ---------------------------------------------------------------------------
// RealJobProcessor — production implementation
// ---------------------------------------------------------------------------

pub struct JobProcessor {
    job_result_destination: String,
    s3: S3DocumentIngestor,
    uploader: ParquetUploader,
    runtime: Arc<Runtime>,
}

impl JobProcessor {
    pub fn new(
        job_result_destination: String,
        s3: S3DocumentIngestor,
        uploader: ParquetUploader,
    ) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            job_result_destination,
            s3,
            uploader,
            runtime: Arc::new(runtime),
        })
    }
}

impl Clone for JobProcessor {
    fn clone(&self) -> Self {
        Self {
            job_result_destination: self.job_result_destination.clone(),
            s3: self.s3.clone(),
            uploader: self.uploader.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl MessageProcessor for JobProcessor {
    type IncomingMessage = JobMessage;

    fn process(
        &self,
        message: Self::IncomingMessage,
    ) -> impl Stream<Item = OutgoingMessage<impl serde::Serialize>> {
        stream! {
            let job = message;
            log::debug!(
                "Starting job {} for resource {} (version {})",
                job.job_id,
                job.resource_id,
                if job.source_version.is_empty() { "legacy" } else { &job.source_version },
            );
            let processing = processing_job(&job);
            yield OutgoingMessage::new(
                self.job_result_destination.clone(),
                job.resource_id.clone(),
                processing
            );

            let runtime = Arc::clone(&self.runtime);
            let uploader = self.uploader.clone();
            let s3 = self.s3.clone();
            let conversion_job = job.clone();
            let result = match tokio::task::spawn_blocking(move || {
                run_conversion(&runtime, &uploader, &conversion_job, &s3)
            }).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => failed_job(&job, error),
                Err(error) => failed_job(&job, error),
            };

            yield OutgoingMessage::new(
                self.job_result_destination.clone(),
                job.resource_id.clone(),
                result
            );
        }
    }
}

fn job_result_from_success(
    job_id: String,
    resource_id: String,
    source_version: String,
    outcome: ckan_ingestor_lib::readers::ckan_reader::SuccessResult,
    artifact: ckan_ingestor_worker_lib::ParquetArtifact,
) -> JobResultMessage {
    JobResultMessage {
        job_id,
        reader: Some(outcome.reader),
        status: JobStatus::Success,
        rows_processed: i64::try_from(outcome.rows_processed).ok(),
        expected_rows: outcome
            .expected_rows
            .and_then(|value| i64::try_from(value).ok()),
        encoding: outcome.encoding,
        csv_strict_mode: outcome.csv_strict_mode,
        csv_delimiter: outcome.csv_delimiter,
        csv_samples: outcome.csv_samples,
        expected_columns: outcome
            .expected_columns
            .and_then(|value| i64::try_from(value).ok()),
        datastore_active: None,
        resource_id,
        source_version: Some(source_version),
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
    let version = if job.source_version.is_empty() {
        job.job_id.as_str()
    } else {
        job.source_version.as_str()
    };
    if runtime.block_on(uploader.exists(&job.resource_id, version))? {
        return Ok(JobResultMessage {
            job_id: job.job_id.clone(),
            status: JobStatus::Success,
            resource_id: job.resource_id.clone(),
            source_version: Some(version.into()),
            dataset_name: None,
            resource_name: None,
            resource_url: None,
            resource_format: None,
            instance_id: None,
            ckan_url: None,
            datastore_active: Some(job.datastore_active),
            reader: Some("cached-parquet".into()),
            rows_processed: None,
            expected_rows: None,
            encoding: None,
            csv_strict_mode: None,
            csv_delimiter: None,
            csv_samples: None,
            expected_columns: None,
            error_message: None,
            preview: None,
            artifact: None,
        });
    }
    let resource = CkanResource {
        id: job.resource_id.clone(),
        package_id: job.package_id.clone(),
        url: job.resource_url.clone(),
        format: job.resource_format.clone(),
        datastore_active: job.datastore_active,
        last_modified: job.source_version.clone(),
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
        Box::new(DatapackageReader::with_client(http_client.clone())),
        Box::new(JsonReader::with_client(http_client)),
        Box::new(FileReferenceReader::new(s3)),
    ]);
    match reader.read(&resource) {
        Ok(result) => {
            let artifact =
                runtime.block_on(uploader.upload(&job.resource_id, version, &result.parquet))?;
            Ok(job_result_from_success(
                job.job_id.clone(),
                job.resource_id.clone(),
                job.source_version.clone(),
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

fn failed_job(job: &JobMessage, error: impl std::fmt::Display) -> JobResultMessage {
    let error_str = format!("{error}");
    let truncated = &error_str[..error_str.len().min(16_000)];
    log::error!("Job {} failed: {}", job.job_id, truncated);
    JobResultMessage {
        reader: Some(String::new()),
        job_id: job.job_id.clone(),
        status: JobStatus::Failed,
        rows_processed: None,
        expected_rows: None,
        encoding: None,
        csv_strict_mode: None,
        csv_delimiter: None,
        csv_samples: None,
        expected_columns: None,
        datastore_active: Some(false),
        resource_id: job.resource_id.clone(),
        source_version: Some(job.source_version.clone()),
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

fn processing_job(job: &JobMessage) -> JobResultMessage {
    JobResultMessage {
        reader: Some(String::new()),
        job_id: job.job_id.clone(),
        status: JobStatus::Processing,
        rows_processed: None,
        expected_rows: None,
        encoding: None,
        csv_strict_mode: None,
        csv_delimiter: None,
        csv_samples: None,
        expected_columns: None,
        datastore_active: Some(false),
        resource_id: job.resource_id.clone(),
        source_version: Some(job.source_version.clone()),
        dataset_name: None,
        resource_name: None,
        resource_url: None,
        resource_format: None,
        instance_id: None,
        ckan_url: None,
        error_message: None,
        preview: None,
        artifact: None,
    }
}

fn job_result_from_failure(
    job_id: &str,
    resource_id: &str,
    datastore_active: bool,
    failed: ckan_ingestor_lib::readers::ckan_reader::FailedResult,
) -> JobResultMessage {
    JobResultMessage {
        job_id: job_id.into(),
        status: JobStatus::Failed,
        resource_id: resource_id.into(),
        source_version: None,
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
        csv_samples: None,
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
            "50000".into(),
            "test-reader".into(),
        )
    }

    #[test]
    fn maps_successful_conversion_to_the_internal_artifact_contract() {
        let result = job_result_from_success(
            "job-1".into(),
            "resource-1".into(),
            "2026-09-02T12:00:00Z".into(),
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
        assert_eq!(result.csv_samples.as_deref(), Some("50000"));
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
