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
use ckan_ingestor_lib::readers::external_resource_crawler_reader::{
    CrawlError, CrawlFanout, DiscoveredChild, ExternalResourceCrawlerReader,
};
use ckan_ingestor_lib::readers::file_reference_reader::FileReferenceReader;
use ckan_ingestor_lib::readers::json_reader::JsonReader;
use ckan_ingestor_lib::readers::logfile_reader::LogfileReader;
use ckan_ingestor_lib::readers::multiple_reader::{HttpFormatResolver, MultipleReader};
use ckan_ingestor_lib::readers::protobuf_reader::ProtobufReader;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use futures::Stream;
use reqwest::blocking::Client;
use source_io::{S3Settings, SourceStorage};
use std::{env, sync::Arc, time::Duration};
use tokio::runtime::Runtime;

use crate::messages::{JobMessage, JobResultMessage, JobStatus};
use crate::parquet_uploader::ParquetUploader;
use message_processor::{MessageProcessor, OutgoingMessage};

pub(crate) fn http_client() -> Result<Client, reqwest::Error> {
    Client::builder()
        .timeout(Duration::from_secs(1200))
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        )
        .build()
}

// ---------------------------------------------------------------------------
// RealJobProcessor — production implementation
// ---------------------------------------------------------------------------

pub struct JobProcessor {
    job_destination: String,
    job_result_destination: String,
    s3: S3DocumentIngestor,
    uploader: ParquetUploader,
    runtime: Arc<Runtime>,
}

impl JobProcessor {
    pub fn new(
        job_destination: String,
        job_result_destination: String,
        s3: S3DocumentIngestor,
        uploader: ParquetUploader,
    ) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            job_destination,
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
            job_destination: self.job_destination.clone(),
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
                &job.source_version,
            );
            let processing = processing_job(&job);
            yield OutgoingMessage::new(
                self.job_result_destination.clone(),
                job.resource_id.clone(),
                WorkerOutput::Result(processing)
            );

            let runtime = Arc::clone(&self.runtime);
            let uploader = self.uploader.clone();
            let s3 = self.s3.clone();
            let conversion_job = job.clone();
            let outcome = match tokio::task::spawn_blocking(move || {
                run_conversion(&runtime, &uploader, &conversion_job, &s3)
            }).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => ConversionOutcome::Result(failed_job(&job, error)),
                Err(error) => ConversionOutcome::Result(failed_job(&job, error)),
            };
            match outcome {
                ConversionOutcome::Result(result) => {
                    yield OutgoingMessage::new(
                        self.job_result_destination.clone(),
                        job.resource_id.clone(),
                        WorkerOutput::Result(result),
                    );
                }
                ConversionOutcome::Children(children) => {
                    let count = u32::try_from(children.len()).expect("bounded child count");
                    let mut manifest = processing_job(&job);
                    manifest.total_child_count = Some(count);
                    yield OutgoingMessage::new(
                        self.job_result_destination.clone(),
                        job.resource_id.clone(),
                        WorkerOutput::Result(manifest),
                    );
                    for (index, child) in children.into_iter().enumerate() {
                        let child_job = child_job(&job, child, index as u32, count);
                        let mut pending = JobResultMessage::pending(
                            &child_job.job_id,
                            &child_job.resource_id,
                            job.dataset_name.as_deref().unwrap_or_default(),
                            job.instance_id.as_deref().unwrap_or_default(),
                        );
                        pending.parent_job_id = child_job.parent_job_id.clone();
                        pending.source_version = Some(child_job.source_version.clone());
                        pending.parent_resource_id = child_job.parent_resource_id.clone();
                        pending.child_index = child_job.child_index;
                        pending.total_child_count = child_job.total_child_count;
                        pending.resource_name = child_job.resource_name.clone();
                        pending.resource_url = Some(child_job.resource_url.clone());
                        pending.resource_format = Some(child_job.resource_format.clone());
                        pending.ckan_url = Some(job.ckan_url.clone());
                        yield OutgoingMessage::new(
                            self.job_result_destination.clone(),
                            job.resource_id.clone(),
                            WorkerOutput::Result(pending),
                        );
                        yield OutgoingMessage::new(
                            self.job_destination.clone(),
                            child_job.resource_id.clone(),
                            WorkerOutput::Job(child_job),
                        );
                    }
                }
            }
        }
    }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum WorkerOutput {
    Job(JobMessage),
    Result(JobResultMessage),
}

#[allow(clippy::large_enum_variant)]
enum ConversionOutcome {
    Result(JobResultMessage),
    Children(Vec<DiscoveredChild>),
}

fn child_job(parent: &JobMessage, child: DiscoveredChild, index: u32, count: u32) -> JobMessage {
    let resource_id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{}:{}", parent.resource_id, child.resource_id).as_bytes(),
    )
    .to_string();
    let job_id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("{}:{resource_id}", parent.job_id).as_bytes(),
    )
    .to_string();
    JobMessage {
        job_id,
        resource_id,
        external_resource_id: None,
        parent_job_id: Some(parent.job_id.clone()),
        parent_resource_id: Some(parent.resource_id.clone()),
        child_index: Some(index),
        total_child_count: Some(count),
        source_version: parent.source_version.clone(),
        package_id: parent.package_id.clone(),
        ckan_url: parent.ckan_url.clone(),
        resource_url: child.url,
        resource_format: child.format,
        csv_delimiter: parent.csv_delimiter.clone(),
        datastore_active: false,
        dataset_name: parent.dataset_name.clone(),
        instance_id: parent.instance_id.clone(),
        resource_name: Some(child.source_label),
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
        parent_job_id: None,
        parent_resource_id: None,
        child_index: None,
        total_child_count: None,
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
        http_status: None,
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
) -> Result<ConversionOutcome, anyhow::Error> {
    let version = conversion_version(job);
    if job.parent_job_id.is_none()
        && runtime.block_on(uploader.exists(&job.resource_id, version))?
    {
        return Ok(ConversionOutcome::Result(JobResultMessage {
            job_id: job.job_id.clone(),
            parent_job_id: job.parent_job_id.clone(),
            parent_resource_id: job.parent_resource_id.clone(),
            child_index: job.child_index,
            total_child_count: job.total_child_count,
            status: JobStatus::Success,
            resource_id: job.resource_id.clone(),
            source_version: Some(version.into()),
            dataset_name: None,
            resource_name: None,
            resource_url: None,
            resource_format: None,
            instance_id: job.instance_id.clone(),
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
            http_status: None,
            error_message: None,
            preview: None,
            artifact: None,
        }));
    }
    let resource = CkanResource {
        id: job
            .external_resource_id
            .clone()
            .unwrap_or_else(|| job.resource_id.clone()),
        package_id: job.package_id.clone(),
        url: job.resource_url.clone(),
        format: job.resource_format.clone(),
        datastore_active: job.datastore_active,
        last_modified: job.source_version.clone(),
    };
    let http_client = s3.http_client();
    let source_storage = SourceStorage::new(http_client.clone(), S3Settings::from_env())?;
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
        Box::new(JsonReader::with_client(http_client.clone())),
        Box::new(ProtobufReader::with_client(http_client.clone())),
        Box::new(LogfileReader::with_client(http_client.clone())),
        Box::new(FileReferenceReader::new(s3)),
        Box::new(ExternalResourceCrawlerReader::new(
            http_client.clone(),
            &source_storage,
            vec![
                Box::new(CsvReader::with_delimiter(
                    http_client.clone(),
                    job.csv_delimiter.clone(),
                )),
                Box::new(JsonReader::with_client(http_client.clone())),
                Box::new(FileReferenceReader::new(s3)),
            ],
            env::var("GOOGLE_DRIVE_API_KEY").unwrap_or_default(),
        )),
    ])
    .with_format_resolver(HttpFormatResolver::new(http_client));
    match reader.read(&resource) {
        Ok(result) => {
            let artifact =
                runtime.block_on(uploader.upload(&job.resource_id, version, &result.parquet))?;
            let mut result = job_result_from_success(
                job.job_id.clone(),
                job.resource_id.clone(),
                job.source_version.clone(),
                result,
                artifact,
            );
            copy_child_context(job, &mut result);
            Ok(ConversionOutcome::Result(result))
        }
        Err(failed) => {
            if let Some(children) = failed
                .error
                .downcast_ref::<CrawlError>()
                .and_then(|error| error.0.downcast_ref::<CrawlFanout>())
            {
                return Ok(ConversionOutcome::Children(children.0.clone()));
            }
            let mut result = if failed.is_deleted() {
                job_result_from_deleted(&job.job_id, &job.resource_id, job.datastore_active, failed)
            } else {
                job_result_from_failure(&job.job_id, &job.resource_id, job.datastore_active, failed)
            };
            copy_child_context(job, &mut result);
            Ok(ConversionOutcome::Result(result))
        }
    }
}

fn conversion_version(job: &JobMessage) -> &str {
    job.source_version.as_str()
}

fn copy_child_context(job: &JobMessage, result: &mut JobResultMessage) {
    result.parent_job_id = job.parent_job_id.clone();
    result.parent_resource_id = job.parent_resource_id.clone();
    result.child_index = job.child_index;
    result.total_child_count = job.total_child_count;
    result.resource_name = job.resource_name.clone();
    result.resource_format = Some(job.resource_format.clone());
    result.instance_id = job.instance_id.clone();
}

fn failed_job(job: &JobMessage, error: impl std::fmt::Display) -> JobResultMessage {
    let error_str = format!("{error}");
    let truncated = &error_str[..error_str.len().min(16_000)];
    log::error!("Job {} failed: {}", job.job_id, truncated);
    JobResultMessage {
        reader: Some(String::new()),
        job_id: job.job_id.clone(),
        parent_job_id: job.parent_job_id.clone(),
        parent_resource_id: job.parent_resource_id.clone(),
        child_index: job.child_index,
        total_child_count: job.total_child_count,
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
        instance_id: job.instance_id.clone(),
        ckan_url: None,
        http_status: None,
        error_message: Some(truncated.to_string()),
        preview: None,
        artifact: None,
    }
}

fn processing_job(job: &JobMessage) -> JobResultMessage {
    JobResultMessage {
        reader: Some(String::new()),
        job_id: job.job_id.clone(),
        parent_job_id: job.parent_job_id.clone(),
        parent_resource_id: job.parent_resource_id.clone(),
        child_index: job.child_index,
        total_child_count: job.total_child_count,
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
        instance_id: job.instance_id.clone(),
        ckan_url: None,
        http_status: None,
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
    let http_status = failed.http_status();
    JobResultMessage {
        job_id: job_id.into(),
        parent_job_id: None,
        parent_resource_id: None,
        child_index: None,
        total_child_count: None,
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
        http_status,
        error_message: Some(failed.error.to_string()),
        preview: Some(vec![]),
        artifact: None,
    }
}

fn job_result_from_deleted(
    job_id: &str,
    resource_id: &str,
    datastore_active: bool,
    failed: ckan_ingestor_lib::readers::ckan_reader::FailedResult,
) -> JobResultMessage {
    JobResultMessage {
        job_id: job_id.into(),
        parent_job_id: None,
        parent_resource_id: None,
        child_index: None,
        total_child_count: None,
        status: JobStatus::Deleted,
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
        rows_processed: None,
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
        http_status: None,
        error_message: None,
        preview: None,
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
        readers::ckan_reader::{FailedResult, HttpStatusError, SuccessResult},
    };
    use ckan_ingestor_worker_lib::ParquetArtifact;
    use reqwest::StatusCode;

    use super::{
        child_job, conversion_version, copy_child_context, failed_job, job_result_from_deleted,
        job_result_from_failure, job_result_from_success, processing_job,
    };
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
    fn child_job_id_is_stable_when_discovery_order_changes() {
        let parent: ckan_ingestor_worker_lib::JobMessage = serde_json::from_value(serde_json::json!({
            "job_id": "parent-job", "resource_id": "parent-resource", "source_version": "v1",
            "package_id": "dataset", "ckan_url": "https://example.test", "resource_url": "https://example.test/page",
            "resource_format": "JSON", "datastore_active": false
        })).unwrap();
        let source =
            || ckan_ingestor_lib::readers::external_resource_crawler_reader::DiscoveredChild {
                resource_id: "same-file".into(),
                url: "https://example.test/file.json".into(),
                source_label: "file.json".into(),
                format: "JSON".into(),
                last_modified: String::new(),
            };
        let first = child_job(&parent, source(), 0, 2);
        let reordered = child_job(&parent, source(), 1, 2);
        assert_eq!(first.job_id, reordered.job_id);
        assert_eq!(first.resource_id, reordered.resource_id);
    }

    #[test]
    fn conversion_uses_an_empty_source_version_as_the_artifact_version() {
        let job: ckan_ingestor_worker_lib::JobMessage = serde_json::from_value(serde_json::json!({
            "job_id": "job-id", "resource_id": "resource-id", "source_version": "",
            "package_id": "dataset", "ckan_url": "https://example.test", "resource_url": "https://example.test/file.csv",
            "resource_format": "CSV", "datastore_active": false
        }))
        .unwrap();

        assert_eq!(conversion_version(&job), "");
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

    #[test]
    fn maps_http_failure_to_the_protocol_http_status() {
        let failed = FailedResult::from(anyhow::Error::new(HttpStatusError::new(
            StatusCode::FORBIDDEN,
            "resource download failed with HTTP status 403 Forbidden",
        )));
        let result = job_result_from_failure("job-1", "resource-1", false, failed);

        assert_eq!(result.http_status, Some(403));
    }

    #[test]
    fn preserves_aggregated_reader_messages_in_the_job_result() {
        let message = "CsvReader: Jev is disable\nCrawler: resource content is not HTML";
        let failed = FailedResult::from_string(message, "Crawler".into());
        let result = job_result_from_failure("job-1", "resource-1", false, failed);
        assert_eq!(result.error_message.as_deref(), Some(message));
        assert_eq!(result.reader.as_deref(), Some("Crawler"));
    }

    #[test]
    fn maps_deleted_conversion_to_the_protocol_deleted_status() {
        let deleted = FailedResult::from_string(
            "resource download failed with HTTP status 404 Not Found",
            "test-reader".into(),
        )
        .into_deleted();
        let result = job_result_from_deleted("job-1", "resource-1", false, deleted);

        assert_eq!(result.status, JobStatus::Deleted);
        assert_eq!(result.resource_id, "resource-1");
        assert!(result.error_message.is_none());
        assert!(result.artifact.is_none());
    }

    #[test]
    fn worker_lifecycle_results_carry_the_job_instance() {
        let job: ckan_ingestor_worker_lib::JobMessage = serde_json::from_value(serde_json::json!({
            "job_id": "job-1", "resource_id": "resource-1", "instance_id": "instance-1",
            "source_version": "v1", "ckan_url": "https://example.test"
        }))
        .unwrap();

        assert_eq!(
            processing_job(&job).instance_id.as_deref(),
            Some("instance-1")
        );
        assert_eq!(
            failed_job(&job, "boom").instance_id.as_deref(),
            Some("instance-1")
        );

        let mut success = job_result_from_success(
            job.job_id.clone(),
            job.resource_id.clone(),
            job.source_version.clone(),
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
        copy_child_context(&job, &mut success);
        assert_eq!(success.instance_id.as_deref(), Some("instance-1"));
    }
}
